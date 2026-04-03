import os
import re
import time
import argparse
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

PRODUCT_LIMIT   = 1000
BATCH_SIZE      = 10
OUTPUT_FILE     = "sample_upload.csv"
TARGET_FILE     = "target.txt"
SHOPIFY_COLUMNS = ["Handle", "Title", "Body (HTML)", "Variant Price", "Image Src"]

SEL_PRODUCT_CARD = (
    "[class*='product-card'], [class*='product-item'], "
    "[class*='ProductCard'], article[data-product], li[data-product-id], "
    "article.product_pod"
)
SEL_TITLE       = "h2, h3, [class*='product-title'], [class*='ProductName']"
SEL_PRICE       = "[class*='price']:not([class*='compare']), [data-price]"
SEL_IMAGE       = "img"
SEL_DESCRIPTION = "[class*='description'], [class*='excerpt'], p"


def load_target_url() -> str:
    target_path = Path(__file__).parent / TARGET_FILE
    if not target_path.exists():
        print(f"[✗] '{TARGET_FILE}' not found.")
        print(f"    Create a file named 'target.txt' and paste your URL inside it.")
        raise SystemExit(1)
    url = target_path.read_text(encoding="utf-8").strip()
    if not url:
        print(f"[✗] 'target.txt' is empty. Paste your URL inside and save.")
        raise SystemExit(1)
    if not url.startswith("http"):
        print(f"[✗] Invalid URL: '{url}'. Must start with https://")
        raise SystemExit(1)
    print(f"[✓] URL loaded: {url}")
    return url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",  type=int, default=PRODUCT_LIMIT)
    parser.add_argument("--batch",  type=int, default=BATCH_SIZE)
    parser.add_argument("--output", default=OUTPUT_FILE)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


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
        print(f"[resume] {len(handles)} products already saved — skipping those.")
        return handles
    except Exception as e:
        print(f"[resume] Could not read existing file ({e}) — starting fresh.")
        return set()


def gemini_format(raw_text: str, retries: int = 2) -> tuple[str, bool]:
    if not raw_text.strip():
        return "<p>No description available.</p>", False

    model  = genai.GenerativeModel("gemini-2.5-flash")
    prompt = (
        "You are a Shopify product copywriter. "
        "Rewrite the following raw product text as clean HTML only. "
        "Use <p> tags for descriptive sentences and <ul><li> tags for feature bullets. "
        "Output ONLY the HTML — no markdown code fences, no explanation, no extra text.\n\n"
        f"RAW TEXT:\n{raw_text}"
    )

    for attempt in range(1, retries + 2):
        try:
            response = model.generate_content(prompt)
            html     = response.text.strip()
            html     = re.sub(r"^```[a-z]*\n?", "", html)
            html     = re.sub(r"\n?```$",        "", html)
            return html.strip(), True
        except Exception as e:
            if attempt <= retries:
                wait = 2 ** attempt
                print(f"    [Gemini] {type(e).__name__} — retrying in {wait}s…")
                time.sleep(wait)
            else:
                print(f"    [Gemini] Failed — using raw text as fallback.")
                safe = raw_text.replace("\n\n", "</p><p>").replace("\n", " ")
                return f"<p>{safe}</p>", False

    return f"<p>{raw_text}</p>", False


def flush_batch(batch: list[dict], output_path: str, first_flush: bool) -> None:
    if not batch:
        return
    df           = pd.DataFrame(batch, columns=SHOPIFY_COLUMNS)
    write_header = first_flush and not Path(output_path).exists()
    df.to_csv(output_path, mode="a", index=False,
              header=write_header, encoding="utf-8-sig")
    print(f"  [disk] Saved {len(batch)} rows → {output_path}")


def scrape_and_save(url, limit, batch_size, output_path, skip_handles) -> int:
    total_written = 0
    batch: list[dict] = []
    first_flush = not Path(output_path).exists()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ))

        print(f"[→] Opening {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)

        try:
            page.wait_for_selector(SEL_PRODUCT_CARD, timeout=20_000)
        except Exception:
            print("[!] No products found. Update SEL_PRODUCT_CARD for this site.")
            browser.close()
            return 0

        for _ in range(5):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.6)

        cards = page.query_selector_all(SEL_PRODUCT_CARD)
        cap   = limit or len(cards)
        print(f"[✓] {len(cards)} products found — processing up to {cap}\n")

        for idx, card in enumerate(cards[:cap], start=1):
            try:
                title_el  = card.query_selector(SEL_TITLE)
                title     = title_el.inner_text().strip() if title_el else "Untitled Product"

                price_el  = card.query_selector(SEL_PRICE)
                price     = clean_price(price_el.inner_text()) if price_el else "0.00"

                img_el    = card.query_selector(SEL_IMAGE)
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

                desc_el  = card.query_selector(SEL_DESCRIPTION)
                raw_desc = desc_el.inner_text().strip() if desc_el else ""

            except Exception as e:
                print(f"  [{idx:04d}] Skipping — DOM error: {type(e).__name__}")
                continue

            handle = slugify(title)
            if handle in skip_handles:
                print(f"  [{idx:04d}] Already saved — skipping: {title}")
                continue

            print(f"  [{idx:04d}] {title}  |  {price}")
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

            if len(batch) >= batch_size:
                flush_batch(batch, output_path, first_flush)
                total_written += len(batch)
                first_flush    = False
                batch          = []

        browser.close()

    if batch:
        flush_batch(batch, output_path, first_flush)
        total_written += len(batch)

    return total_written


if __name__ == "__main__":
    args         = parse_args()
    url          = load_target_url()
    skip_handles = already_scraped_handles(args.output) if args.resume else set()

    if not args.resume and Path(args.output).exists():
        Path(args.output).unlink()
        print(f"[init] Old {args.output} removed — starting fresh.\n")

    written = scrape_and_save(
        url          = url,
        limit        = args.limit,
        batch_size   = args.batch,
        output_path  = args.output,
        skip_handles = skip_handles,
    )

    if written == 0:
        print("\n[✗] Nothing written. Check selectors or target URL.")
        raise SystemExit(1)

    print(f"\n[✓] Complete — {written} products saved to {args.output}")
