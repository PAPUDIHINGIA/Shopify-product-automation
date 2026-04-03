import os
import re
import time
import argparse
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

# ── DEFAULTS (overridden by CLI flags) ────────────────────────────────────────
PRODUCT_LIMIT   = 1000
BATCH_SIZE      = 10
OUTPUT_FILE     = "sample_upload.csv"
TARGET_FILE     = "target.txt"
SHOPIFY_COLUMNS = ["Handle", "Title", "Body (HTML)", "Variant Price", "Image Src"]

# Default CSS selectors — override any of these with CLI flags
DEFAULT_SELECTORS = {
    "card":  (
        "[class*='product-card'], [class*='product-item'], "
        "[class*='ProductCard'], article[data-product], li[data-product-id]"
    ),
    "title": "h2, h3, [class*='product-title'], [class*='ProductName']",
    "price": "[class*='price']:not([class*='compare']), [data-price]",
    "image": "img",
    "desc":  "[class*='description'], [class*='excerpt'], p",
}

# ── STEALTH HEADERS (reduces bot detection) ───────────────────────────────────
STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
STEALTH_VIEWPORT  = {"width": 1920, "height": 1080}
STEALTH_HEADERS   = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Enterprise Shopify Scraper",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # ── Output / run control ──────────────────────────────────────────────────
    p.add_argument("--limit",  type=int, default=PRODUCT_LIMIT,
                   help="Max total products to scrape (default: 1000)")
    p.add_argument("--batch",  type=int, default=BATCH_SIZE,
                   help="Rows flushed to disk at once (default: 10)")
    p.add_argument("--output", default=OUTPUT_FILE,
                   help="Output CSV filename (default: sample_upload.csv)")
    p.add_argument("--resume", action="store_true",
                   help="Append to existing CSV — skip already-saved handles")

    # ── CSS selector overrides ────────────────────────────────────────────────
    p.add_argument("--card-sel",  default=None,
                   help="CSS selector for product card container")
    p.add_argument("--title-sel", default=None,
                   help="CSS selector for product title  e.g. 'h2.product-title'")
    p.add_argument("--price-sel", default=None,
                   help="CSS selector for product price  e.g. '.price-tag'")
    p.add_argument("--image-sel", default=None,
                   help="CSS selector for product image  e.g. '.card-img img'")
    p.add_argument("--desc-sel",  default=None,
                   help="CSS selector for description    e.g. '.product-desc'")

    # ── Pagination — next-button mode ─────────────────────────────────────────
    p.add_argument("--next-sel", default=None,
                   help=(
                       "CSS selector for the 'Next Page' button/link.\n"
                       "Example: --next-sel \"a.pagination__next\"\n"
                       "The engine clicks it repeatedly until it disappears\n"
                       "or --limit is reached."
                   ))

    # ── Pagination — URL pattern mode ─────────────────────────────────────────
    p.add_argument("--url-pattern", default=None,
                   help=(
                       "URL template with {} as the page number placeholder.\n"
                       "Example: --url-pattern \"https://site.com/shop?page={}\"\n"
                       "Use with --page-start and --page-end."
                   ))
    p.add_argument("--page-start", type=int, default=1,
                   help="First page number for --url-pattern (default: 1)")
    p.add_argument("--page-end",   type=int, default=50,
                   help="Last  page number for --url-pattern (default: 50)")

    return p.parse_args()


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def load_target_url() -> str:
    target_path = Path(__file__).parent / TARGET_FILE
    if not target_path.exists():
        print(f"[✗] '{TARGET_FILE}' not found.")
        print("    Create target.txt and paste your URL on line 1.")
        raise SystemExit(1)
    url = target_path.read_text(encoding="utf-8").strip()
    if not url:
        print("[✗] target.txt is empty.")
        raise SystemExit(1)
    if not url.startswith("http"):
        print(f"[✗] Invalid URL: '{url}'")
        raise SystemExit(1)
    print(f"[✓] Base URL loaded: {url}")
    return url


def build_selectors(args: argparse.Namespace) -> dict:
    """Merge CLI overrides on top of defaults."""
    return {
        "card":  args.card_sel  or DEFAULT_SELECTORS["card"],
        "title": args.title_sel or DEFAULT_SELECTORS["title"],
        "price": args.price_sel or DEFAULT_SELECTORS["price"],
        "image": args.image_sel or DEFAULT_SELECTORS["image"],
        "desc":  args.desc_sel  or DEFAULT_SELECTORS["desc"],
    }


def slugify(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def clean_price(raw: str) -> str:
    return re.sub(r"[^\d.]", "", raw).strip() or "0.00"


def already_scraped_handles(output_path: str) -> set:
    p = Path(output_path)
    if not p.exists():
        return set()
    try:
        existing = pd.read_csv(p, usecols=["Handle"])
        handles  = set(existing["Handle"].dropna().tolist())
        print(f"[resume] {len(handles)} handles already saved — will skip them.")
        return handles
    except Exception as e:
        print(f"[resume] Could not read file ({e}) — starting fresh.")
        return set()


# ══════════════════════════════════════════════════════════════════════════════
# GEMINI (bulletproof — never crashes the pipeline)
# ══════════════════════════════════════════════════════════════════════════════
def gemini_format(raw_text: str, retries: int = 2) -> tuple[str, bool]:
    if not raw_text.strip():
        return "<p>No description available.</p>", False

    model  = genai.GenerativeModel("gemini-2.5-flash")
    prompt = (
        "You are a Shopify product copywriter. "
        "Rewrite the following raw product text as clean HTML only. "
        "Use <p> tags for descriptive sentences and <ul><li> tags for feature bullets. "
        "Output ONLY the HTML — no markdown code fences, no explanation.\n\n"
        f"RAW TEXT:\n{raw_text}"
    )

    for attempt in range(1, retries + 2):
        try:
            html = model.generate_content(prompt).text.strip()
            html = re.sub(r"^```[a-z]*\n?", "", html)
            html = re.sub(r"\n?```$",        "", html)
            return html.strip(), True
        except Exception as e:
            if attempt <= retries:
                wait = 2 ** attempt
                print(f"    [Gemini] {type(e).__name__} — retry in {wait}s…")
                time.sleep(wait)
            else:
                print(f"    [Gemini] All retries failed — raw fallback.")
                safe = raw_text.replace("\n\n", "</p><p>").replace("\n", " ")
                return f"<p>{safe}</p>", False

    return f"<p>{raw_text}</p>", False


# ══════════════════════════════════════════════════════════════════════════════
# BATCH WRITER (append mode — crash-safe)
# ══════════════════════════════════════════════════════════════════════════════
def flush_batch(batch: list[dict], output_path: str, first_flush: bool) -> None:
    if not batch:
        return
    df           = pd.DataFrame(batch, columns=SHOPIFY_COLUMNS)
    write_header = first_flush and not Path(output_path).exists()
    df.to_csv(output_path, mode="a", index=False,
              header=write_header, encoding="utf-8-sig")
    print(f"  [disk] ✓ Flushed {len(batch)} rows → {output_path}")


# ══════════════════════════════════════════════════════════════════════════════
# CARD EXTRACTOR  (shared by both pagination modes)
# ══════════════════════════════════════════════════════════════════════════════
def extract_cards(page, sels: dict, skip_handles: set,
                  batch: list, first_flush: bool,
                  total_written: int, batch_size: int,
                  output_path: str, limit: int,
                  page_num: int) -> tuple[list, bool, int, bool]:
    """
    Extracts all product cards from the current page DOM.
    Returns updated (batch, first_flush, total_written, limit_reached).
    """
    # Scroll to trigger lazy-loaded images
    for _ in range(4):
        page.evaluate("window.scrollBy(0, window.innerHeight)")
        time.sleep(0.5)

    cards = page.query_selector_all(sels["card"])
    print(f"  [page {page_num}] {len(cards)} cards found")

    for idx, card in enumerate(cards, start=1):
        if total_written + len(batch) >= limit:
            print(f"  [limit] Reached {limit} products — stopping.")
            return batch, first_flush, total_written, True   # limit_reached=True

        # ── DOM extraction (per-card try/except — never skip the whole page) ──
        try:
            title_el  = card.query_selector(sels["title"])
            title     = title_el.inner_text().strip() if title_el else "Untitled Product"

            price_el  = card.query_selector(sels["price"])
            price     = clean_price(price_el.inner_text()) if price_el else "0.00"

            img_el    = card.query_selector(sels["image"])
            image_url = ""
            if img_el:
                image_url = (
                    img_el.get_attribute("src")
                    or img_el.get_attribute("data-src")
                    or img_el.get_attribute("data-lazy-src")
                    or ""
                )
            if image_url.startswith("//"):
                image_url = "https:" + image_url

            desc_el  = card.query_selector(sels["desc"])
            raw_desc = desc_el.inner_text().strip() if desc_el else ""

        except Exception as e:
            print(f"    [card {idx}] DOM error ({type(e).__name__}) — skipping.")
            continue

        handle = slugify(title)
        if handle in skip_handles:
            print(f"    [card {idx}] Already saved — skipping: {title}")
            continue

        # ── Gemini enrichment ─────────────────────────────────────────────────
        running_total = total_written + len(batch) + 1
        print(f"  [{running_total:04d}] {title}  |  {price}")
        body_html, used_ai = gemini_format(raw_desc)
        print(f"         Description: [{'AI' if used_ai else 'RAW FALLBACK'}]")

        batch.append({
            "Handle":        handle,
            "Title":         title,
            "Body (HTML)":   body_html,
            "Variant Price": price,
            "Image Src":     image_url,
        })
        skip_handles.add(handle)

        # ── Flush when batch is full ──────────────────────────────────────────
        if len(batch) >= batch_size:
            flush_batch(batch, output_path, first_flush)
            total_written += len(batch)
            first_flush    = False
            batch          = []

    return batch, first_flush, total_written, False


# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION ENGINE — MODE 1: NEXT BUTTON
# ══════════════════════════════════════════════════════════════════════════════
def scrape_next_button(browser, base_url: str, next_sel: str,
                       sels: dict, skip_handles: set,
                       batch_size: int, output_path: str, limit: int) -> int:
    """
    Loads base_url, scrapes cards, clicks the Next button, repeats.
    Stops when Next button disappears or limit is reached.

    Usage example:
        python "real wold.py" --next-sel "a.pagination__next"
        python "real wold.py" --next-sel "li.next > a"
        python "real wold.py" --next-sel "[aria-label='Next page']"
    """
    total_written = 0
    batch: list[dict] = []
    first_flush = not Path(output_path).exists()
    page_num    = 0

    page = browser.new_page(
        user_agent=STEALTH_UA,
        viewport=STEALTH_VIEWPORT,
        extra_http_headers=STEALTH_HEADERS,
    )
    # Stealth: mask WebDriver property
    page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

    print(f"\n[→] Opening {base_url}")
    page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)

    while True:
        page_num += 1
        print(f"\n── Page {page_num} ─────────────────────────────────────────────")

        try:
            page.wait_for_selector(sels["card"], timeout=20_000)
        except PWTimeout:
            print(f"  [!] No product cards found on page {page_num} — stopping.")
            break

        batch, first_flush, total_written, hit_limit = extract_cards(
            page, sels, skip_handles, batch, first_flush,
            total_written, batch_size, output_path, limit, page_num
        )
        if hit_limit:
            break

        # ── Try to click Next ─────────────────────────────────────────────────
        next_btn = page.query_selector(next_sel)
        if not next_btn:
            print(f"\n[✓] No '{next_sel}' found — reached last page.")
            break

        # Check it isn't disabled (Shopify/WooCommerce pattern)
        is_disabled = (
            next_btn.get_attribute("aria-disabled") == "true"
            or "disabled" in (next_btn.get_attribute("class") or "")
        )
        if is_disabled:
            print("\n[✓] Next button is disabled — reached last page.")
            break

        print(f"  [nav] Clicking next page button…")
        next_btn.scroll_into_view_if_needed()
        next_btn.click()
        page.wait_for_load_state("domcontentloaded")
        time.sleep(1.5)   # polite pause — avoids rate-limiting

    # Final partial batch
    if batch:
        flush_batch(batch, output_path, first_flush)
        total_written += len(batch)

    page.close()
    return total_written


# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION ENGINE — MODE 2: URL PATTERN
# ══════════════════════════════════════════════════════════════════════════════
def scrape_url_pattern(browser, url_pattern: str,
                       page_start: int, page_end: int,
                       sels: dict, skip_handles: set,
                       batch_size: int, output_path: str, limit: int) -> int:
    """
    Iterates through URL pages built from a pattern.
    Stops early if a page returns no products (catalog end).

    Usage examples:
        --url-pattern "https://site.com/shop?page={}" --page-start 1 --page-end 50
        --url-pattern "https://site.com/catalog/page/{}" --page-start 1 --page-end 100
        --url-pattern "https://site.com/products?offset={}" --page-start 0 --page-end 990
    """
    total_written = 0
    batch: list[dict] = []
    first_flush = not Path(output_path).exists()

    page = browser.new_page(
        user_agent=STEALTH_UA,
        viewport=STEALTH_VIEWPORT,
        extra_http_headers=STEALTH_HEADERS,
    )
    page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

    for page_num in range(page_start, page_end + 1):
        url = url_pattern.format(page_num)
        print(f"\n── Page {page_num} → {url} {'─' * 30}")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_selector(sels["card"], timeout=15_000)
        except PWTimeout:
            print(f"  [✓] No products on page {page_num} — catalog end reached.")
            break
        except Exception as e:
            print(f"  [!] Failed to load page {page_num} ({type(e).__name__}) — skipping.")
            time.sleep(2)
            continue

        prev_total = total_written + len(batch)

        batch, first_flush, total_written, hit_limit = extract_cards(
            page, sels, skip_handles, batch, first_flush,
            total_written, batch_size, output_path, limit, page_num
        )

        # If this page added zero new products, we've hit the real end
        if (total_written + len(batch)) == prev_total:
            print(f"  [✓] Page {page_num} returned 0 new products — stopping.")
            break

        if hit_limit:
            break

        time.sleep(1.2)   # polite crawl delay

    # Final partial batch
    if batch:
        flush_batch(batch, output_path, first_flush)
        total_written += len(batch)

    page.close()
    return total_written


# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION ENGINE — MODE 3: SINGLE PAGE (original scroll behaviour)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_single_page(browser, url: str,
                       sels: dict, skip_handles: set,
                       batch_size: int, output_path: str, limit: int) -> int:
    """
    Original single-URL scroll mode — used when no pagination flag is given.
    """
    total_written = 0
    batch: list[dict] = []
    first_flush = not Path(output_path).exists()

    page = browser.new_page(
        user_agent=STEALTH_UA,
        viewport=STEALTH_VIEWPORT,
        extra_http_headers=STEALTH_HEADERS,
    )
    page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

    print(f"[→] Opening {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    try:
        page.wait_for_selector(sels["card"], timeout=20_000)
    except PWTimeout:
        print("[!] No product cards found. Update --card-sel for this site.")
        page.close()
        return 0

    batch, first_flush, total_written, _ = extract_cards(
        page, sels, skip_handles, batch, first_flush,
        total_written, batch_size, output_path, limit, page_num=1
    )

    if batch:
        flush_batch(batch, output_path, first_flush)
        total_written += len(batch)

    page.close()
    return total_written


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    args         = parse_args()
    sels         = build_selectors(args)
    skip_handles = already_scraped_handles(args.output) if args.resume else set()

    if not args.resume and Path(args.output).exists():
        Path(args.output).unlink()
        print(f"[init] Old {args.output} removed — starting fresh.\n")

    # Print active selector config so you can verify before a long run
    print("\n[config] Active selectors:")
    for k, v in sels.items():
        print(f"  {k:6s} → {v}")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",  # stealth
                "--disable-dev-shm-usage",
            ]
        )

        # ── Route to correct pagination engine ────────────────────────────────
        if args.url_pattern:
            # MODE 2 — explicit URL pattern
            print(f"\n[mode] URL Pattern  "
                  f"pages {args.page_start}–{args.page_end}\n")
            written = scrape_url_pattern(
                browser      = browser,
                url_pattern  = args.url_pattern,
                page_start   = args.page_start,
                page_end     = args.page_end,
                sels         = sels,
                skip_handles = skip_handles,
                batch_size   = args.batch,
                output_path  = args.output,
                limit        = args.limit,
            )

        elif args.next_sel:
            # MODE 1 — next button click
            base_url = load_target_url()
            print(f"\n[mode] Next-Button Pagination  selector: '{args.next_sel}'\n")
            written = scrape_next_button(
                browser      = browser,
                base_url     = base_url,
                next_sel     = args.next_sel,
                sels         = sels,
                skip_handles = skip_handles,
                batch_size   = args.batch,
                output_path  = args.output,
                limit        = args.limit,
            )

        else:
            # MODE 3 — single page (original behaviour)
            base_url = load_target_url()
            print(f"\n[mode] Single Page\n")
            written = scrape_single_page(
                browser      = browser,
                url          = base_url,
                sels         = sels,
                skip_handles = skip_handles,
                batch_size   = args.batch,
                output_path  = args.output,
                limit        = args.limit,
            )

        browser.close()

    if written == 0:
        print("\n[✗] Nothing written. Check your selectors or URL.")
        raise SystemExit(1)

    print(f"\n{'═'*60}")
    print(f"  [✓] COMPLETE — {written} products saved to {args.output}")
    print(f"{'═'*60}")