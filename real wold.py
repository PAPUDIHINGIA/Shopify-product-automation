import os
import re
import time
import argparse
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

# ══════════════════════════════════════════════════════════════════════════════
# COLUMN DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

SHOPIFY_COLUMNS = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Option1 Name",
    "Option1 Value",
    "Variant SKU",
    "Variant Price",
    "Image Src",
]

CLIENT_COLUMNS = [
    "Product Handle (URL Slug)",
    "Product Title",
    "Full Description (HTML)",
    "Option Name (e.g. Size / Color / Weight)",
    "Option Value (e.g. Small / Red / 500g)",
    "Variant SKU Code",
    "Price",
    "Image URL (Click to Preview)",
]

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

PRODUCT_LIMIT = 1000
BATCH_SIZE    = 10
OUTPUT_FILE   = "sample_upload.csv"
TARGET_FILE   = "target.txt"

STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
STEALTH_VIEWPORT = {"width": 1920, "height": 1080}
STEALTH_HEADERS  = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DEFAULT_SELECTORS = {
    "card": (
        "[class*='product-card'], [class*='product-item'], "
        "[class*='ProductCard'], [class*='product-tile'], "
        "article[data-product], li[data-product-id], "
        "[class*='product-grid-item'], [class*='productgrid--item']"
    ),
    "link":       "a",
    "title":      "h1, [class*='product-title'], [class*='product__title'], [class*='ProductTitle'], [itemprop='name']",
    "price":      "[class*='price']:not([class*='compare']):not([class*='was']), [data-price], [itemprop='price']",
    "desc":       "[class*='product-description'], [class*='product__description'], [class*='description__text'], #product-description, [itemprop='description']",
    "main_img":   "[class*='product-image'] img, [class*='product__image'] img, [class*='ProductImage'] img, .product__media img",
    "extra_imgs": "[class*='product-image'] img, [class*='product__image'] img, [class*='thumbnail'] img, [class*='gallery'] img",
    "variants":   "select[name*='option'] option, [class*='variant'] input[type='radio'], [class*='swatch'] input",
}

# ══════════════════════════════════════════════════════════════════════════════
# CLI ARGUMENTS
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Universal E-Commerce Scraper → Shopify CSV")
    p.add_argument("--limit",           type=int, default=PRODUCT_LIMIT)
    p.add_argument("--batch",           type=int, default=BATCH_SIZE)
    p.add_argument("--output",          default=OUTPUT_FILE)
    p.add_argument("--resume",          action="store_true")
    p.add_argument("--no-product-page", action="store_true")
    p.add_argument("--card-sel",        default=None)
    p.add_argument("--link-sel",        default=None)
    p.add_argument("--title-sel",       default=None)
    p.add_argument("--price-sel",       default=None)
    p.add_argument("--desc-sel",        default=None)
    p.add_argument("--main-img-sel",    default=None)
    p.add_argument("--extra-imgs-sel",  default=None)
    p.add_argument("--variant-sel",     default=None)
    p.add_argument("--next-sel",        default=None)
    p.add_argument("--url-pattern",     default=None)
    p.add_argument("--page-start",      type=int, default=1)
    p.add_argument("--page-end",        type=int, default=50)
    return p.parse_args()

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def load_target_url() -> str:
    target_path = Path(__file__).parent / TARGET_FILE
    if not target_path.exists():
        print(f"\n[X] target.txt not found.")
        print(f"    Create target.txt in this folder and paste your URL inside.")
        raise SystemExit(1)
    url = target_path.read_text(encoding="utf-8").strip()
    if not url:
        print("[X] target.txt is empty. Paste your URL inside and save.")
        raise SystemExit(1)
    if not url.startswith("http"):
        print(f"[X] Invalid URL: '{url}' — must start with https://")
        raise SystemExit(1)
    print(f"[OK] Target URL: {url}")
    return url


def build_selectors(args: argparse.Namespace) -> dict:
    return {
        "card":       args.card_sel       or DEFAULT_SELECTORS["card"],
        "link":       args.link_sel       or DEFAULT_SELECTORS["link"],
        "title":      args.title_sel      or DEFAULT_SELECTORS["title"],
        "price":      args.price_sel      or DEFAULT_SELECTORS["price"],
        "desc":       args.desc_sel       or DEFAULT_SELECTORS["desc"],
        "main_img":   args.main_img_sel   or DEFAULT_SELECTORS["main_img"],
        "extra_imgs": args.extra_imgs_sel or DEFAULT_SELECTORS["extra_imgs"],
        "variants":   args.variant_sel    or DEFAULT_SELECTORS["variants"],
    }


def slugify(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def clean_price(raw: str) -> str:
    cleaned = re.sub(r"[^\d.]", "", raw).strip()
    return cleaned or "0.00"


def resolve_url(href: str, base_url: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return href


def clean_image_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    return url


def already_scraped_handles(output_path: str) -> set:
    p = Path(output_path)
    if not p.exists():
        return set()
    try:
        existing = pd.read_csv(p, usecols=["Handle"])
        handles  = set(existing["Handle"].dropna().tolist())
        print(f"[resume] {len(handles)} products already saved — skipping.")
        return handles
    except Exception as e:
        print(f"[resume] Could not read file ({e}) — starting fresh.")
        return set()


def derive_review_path(output_path: str) -> str:
    stem = Path(output_path).stem
    return str(Path(output_path).parent / f"{stem}_client_review.csv")


def new_stealth_page(browser):
    page = browser.new_page(
        user_agent         = STEALTH_UA,
        viewport           = STEALTH_VIEWPORT,
        extra_http_headers = STEALTH_HEADERS,
    )
    page.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    return page

# ══════════════════════════════════════════════════════════════════════════════
# GEMINI AI
# ══════════════════════════════════════════════════════════════════════════════

def gemini_format(raw_text: str, retries: int = 2) -> tuple[str, bool]:
    if not raw_text.strip():
        return "<p>No description available.</p>", False

    model  = genai.GenerativeModel("gemini-2.5-flash")
    prompt = (
        "You are a Shopify product copywriter. "
        "Rewrite the following raw product text as clean HTML only. "
        "Use <p> tags for descriptive sentences. "
        "Use <ul><li> tags for features and specifications. "
        "Output ONLY the HTML — no markdown, no code fences, no explanation.\n\n"
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
                print(f"    [Gemini] {type(e).__name__} — retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    [Gemini] All retries failed — using raw text fallback.")
                safe = raw_text.replace("\n\n", "</p><p>").replace("\n", " ")
                return f"<p>{safe}</p>", False

    return f"<p>{raw_text}</p>", False

# ══════════════════════════════════════════════════════════════════════════════
# VARIANT EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

def extract_variants(page, base_price: str, sels: dict) -> list[dict]:
    variants = []

    # Method 1: Shopify JSON
    try:
        product_data = page.evaluate("""
            () => {
                if (window.ShopifyAnalytics && window.ShopifyAnalytics.meta && window.ShopifyAnalytics.meta.product) {
                    return window.ShopifyAnalytics.meta.product;
                }
                if (window.__st && window.__st.p) { return window.__st.p; }
                const scripts = document.querySelectorAll('script[type="application/json"], script[id*="product"]');
                for (let s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && d.variants && d.variants.length > 0) return d;
                    } catch(e) {}
                }
                return null;
            }
        """)
        if product_data and isinstance(product_data, dict):
            raw_variants = product_data.get("variants", [])
            options      = product_data.get("options", ["Title"])
            option_name  = options[0] if options else "Title"
            for v in raw_variants:
                raw_p = v.get("price", 0)
                price = f"{int(raw_p) / 100:.2f}" if raw_p else base_price
                variants.append({
                    "name":  option_name,
                    "value": v.get("option1") or v.get("title") or "Default",
                    "sku":   v.get("sku", ""),
                    "price": price,
                })
            if variants:
                print(f"    [variants] {len(variants)} found via Shopify JSON")
                return variants
    except Exception:
        pass

    # Method 2: WooCommerce JSON
    try:
        woo_data = page.evaluate("""
            () => {
                const form = document.querySelector('form.variations_form');
                if (!form) return null;
                const raw = form.getAttribute('data-product_variations');
                return raw ? JSON.parse(raw) : null;
            }
        """)
        if woo_data and isinstance(woo_data, list) and len(woo_data) > 0:
            for v in woo_data:
                attrs     = v.get("attributes", {})
                attr_keys = list(attrs.keys())
                attr_name = attr_keys[0].replace("attribute_pa_", "").replace("attribute_", "").title() if attr_keys else "Option"
                attr_val  = list(attrs.values())[0] if attrs else "Default"
                variants.append({
                    "name":  attr_name,
                    "value": attr_val,
                    "sku":   v.get("sku", ""),
                    "price": str(v.get("display_price", base_price)),
                })
            if variants:
                print(f"    [variants] {len(variants)} found via WooCommerce JSON")
                return variants
    except Exception:
        pass

    # Method 3: DOM fallback
    try:
        option_els = page.query_selector_all(sels["variants"])
        seen = set()
        for el in option_els:
            value = (el.get_attribute("value") or el.inner_text()).strip()
            if not value or value.lower() in ("", "choose", "select", "pick", "choose an option", "select an option"):
                continue
            if value not in seen:
                seen.add(value)
                variants.append({"name": "Option1", "value": value, "sku": "", "price": base_price})
        if variants:
            print(f"    [variants] {len(variants)} found via DOM")
            return variants
    except Exception:
        pass

    print(f"    [variants] None found — using default single variant")
    return [{"name": "Title", "value": "Default Title", "sku": "", "price": base_price}]

# ══════════════════════════════════════════════════════════════════════════════
# IMAGE EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

def extract_images(page, sels: dict) -> tuple[str, list[str]]:
    all_images = []
    try:
        img_els = page.query_selector_all(sels["extra_imgs"])
        for el in img_els:
            src = (
                el.get_attribute("data-zoom-src")
                or el.get_attribute("data-large-src")
                or el.get_attribute("data-src")
                or el.get_attribute("src")
                or ""
            )
            src = clean_image_url(src)
            if not src:
                continue
            skip_keywords = ["placeholder", "blank", "loading", "svg+xml", "spinner", "transparent", "data:image"]
            if any(kw in src.lower() for kw in skip_keywords):
                continue
            width_attr = el.get_attribute("width")
            if width_attr and str(width_attr).isdigit() and int(width_attr) < 80:
                continue
            if src not in all_images:
                all_images.append(src)
    except Exception:
        pass

    if not all_images:
        try:
            main_el = page.query_selector(sels["main_img"])
            if main_el:
                src = clean_image_url(
                    main_el.get_attribute("data-zoom-src")
                    or main_el.get_attribute("data-src")
                    or main_el.get_attribute("src")
                    or ""
                )
                if src:
                    all_images.append(src)
        except Exception:
            pass

    main_image   = all_images[0] if all_images else ""
    extra_images = list(dict.fromkeys(all_images[1:]))
    return main_image, extra_images

# ══════════════════════════════════════════════════════════════════════════════
# PRODUCT PAGE SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape_product_page(product_page, url: str, sels: dict) -> dict:
    try:
        product_page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(0.8)

        title_el = product_page.query_selector(sels["title"])
        title    = title_el.inner_text().strip() if title_el else ""

        price_el = product_page.query_selector(sels["price"])
        price    = clean_price(price_el.inner_text()) if price_el else "0.00"

        desc_el  = product_page.query_selector(sels["desc"])
        raw_desc = desc_el.inner_text().strip() if desc_el else ""

        main_image, extra_images = extract_images(product_page, sels)
        variants = extract_variants(product_page, price, sels)

        return {
            "title":        title,
            "price":        price,
            "raw_desc":     raw_desc,
            "main_image":   main_image,
            "extra_images": extra_images,
            "variants":     variants,
        }
    except Exception as e:
        print(f"    [product page] Failed ({type(e).__name__}) — using card data.")
        return {}

# ══════════════════════════════════════════════════════════════════════════════
# ROW BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_product_rows(handle, title, body_html, price, main_image, extra_images, variants) -> list[dict]:
    rows     = []
    is_first = True

    def empty_row() -> dict:
        return {col: "" for col in SHOPIFY_COLUMNS} | {"Handle": handle}

    for i, variant in enumerate(variants):
        row = empty_row()
        row["Option1 Name"]  = variant.get("name",  "Title")
        row["Option1 Value"] = variant.get("value", "Default Title")
        row["Variant SKU"]   = variant.get("sku",   "")
        row["Variant Price"] = variant.get("price", price)
        if is_first:
            row["Title"]       = title
            row["Body (HTML)"] = body_html
            row["Image Src"]   = main_image
            is_first = False
        else:
            img_index = i - 1
            if img_index < len(extra_images):
                row["Image Src"] = extra_images[img_index]
        rows.append(row)

    images_used = max(0, len(variants) - 1)
    for img in extra_images[images_used:]:
        row = empty_row()
        row["Image Src"] = img
        rows.append(row)

    return rows

# ══════════════════════════════════════════════════════════════════════════════
# BATCH WRITER
# ══════════════════════════════════════════════════════════════════════════════

def flush_batch(batch: list[dict], output_path: str, review_path: str, first_flush: bool) -> None:
    if not batch:
        return

    df_shopify   = pd.DataFrame(batch, columns=SHOPIFY_COLUMNS)
    write_header = first_flush and not Path(output_path).exists()
    df_shopify.to_csv(output_path, mode="a", index=False, header=write_header, encoding="utf-8-sig")

    df_client         = df_shopify.copy()
    df_client.columns = CLIENT_COLUMNS
    df_client["Image URL (Click to Preview)"] = df_client["Image URL (Click to Preview)"].apply(
        lambda url: f'=HYPERLINK("{url}","View Image")' if isinstance(url, str) and url.startswith("http") else url
    )
    write_header_client = first_flush and not Path(review_path).exists()
    df_client.to_csv(review_path, mode="a", index=False, header=write_header_client, encoding="utf-8-sig")

    unique_products = df_shopify["Handle"].nunique()
    print(f"\n  [saved] {unique_products} products ({len(batch)} rows)")
    print(f"  [file1] {output_path}")
    print(f"  [file2] {review_path}")

# ══════════════════════════════════════════════════════════════════════════════
# CARD EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

def extract_cards(catalog_page, product_page, base_url, sels, skip_handles,
                  batch, first_flush, total_written, batch_size,
                  output_path, review_path, limit, page_num, use_product_page):

    for _ in range(4):
        catalog_page.evaluate("window.scrollBy(0, window.innerHeight)")
        time.sleep(0.5)

    cards = catalog_page.query_selector_all(sels["card"])
    print(f"  [page {page_num}] {len(cards)} cards found")

    if len(cards) == 0:
        print(f"  [!] No cards found. Try: python \"real wold.py\" --card-sel \".your-class\"")

    for card in cards:
        if total_written + len(batch) >= limit:
            print(f"  [limit] Reached {limit} — stopping.")
            return batch, first_flush, total_written, True

        try:
            title_el   = card.query_selector(sels["title"])
            card_title = title_el.inner_text().strip() if title_el else ""

            price_el   = card.query_selector(sels["price"])
            card_price = clean_price(price_el.inner_text()) if price_el else "0.00"

            img_el     = card.query_selector("img")
            card_image = ""
            if img_el:
                card_image = clean_image_url(
                    img_el.get_attribute("src") or img_el.get_attribute("data-src") or ""
                )

            desc_el   = card.query_selector(sels["desc"])
            card_desc = desc_el.inner_text().strip() if desc_el else ""

            link_el  = card.query_selector(sels["link"])
            href     = link_el.get_attribute("href") if link_el else ""
            full_url = resolve_url(href, base_url)

        except Exception as e:
            print(f"    [!] Card error ({type(e).__name__}) — skipping.")
            continue

        temp_handle = slugify(card_title or "untitled")
        if temp_handle in skip_handles:
            print(f"    [skip] {card_title}")
            continue

        running_total = total_written + len(batch) + 1
        print(f"\n  ── Product {running_total:04d} ──────────────────────────────")

        page_data = {}
        if use_product_page and full_url:
            print(f"  [url]  {full_url}")
            page_data = scrape_product_page(product_page, full_url, sels)
        else:
            print(f"  [mode] Card data only")

        final_title  = page_data.get("title")     or card_title  or "Untitled Product"
        final_price  = page_data.get("price")     or card_price  or "0.00"
        final_desc   = page_data.get("raw_desc")  or card_desc   or ""
        main_image   = page_data.get("main_image") or card_image or ""
        extra_images = page_data.get("extra_images", [])
        variants     = page_data.get("variants") or [{"name": "Title", "value": "Default Title", "sku": "", "price": final_price}]
        final_handle = slugify(final_title)

        print(f"  [name] {final_title}")
        print(f"  [price] {final_price}  [images] {1 + len(extra_images)}  [variants] {len(variants)}")

        body_html, used_ai = gemini_format(final_desc)
        print(f"  [AI]   {'Formatted OK' if used_ai else 'Raw fallback used'}")

        product_rows = build_product_rows(
            final_handle, final_title, body_html,
            final_price, main_image, extra_images, variants
        )

        batch.extend(product_rows)
        skip_handles.add(final_handle)
        print(f"  [rows] {len(product_rows)} rows added")

        if len(batch) >= batch_size:
            flush_batch(batch, output_path, review_path, first_flush)
            total_written += len(batch)
            first_flush    = False
            batch          = []

    return batch, first_flush, total_written, False

# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION — MODE 1: NEXT BUTTON
# ══════════════════════════════════════════════════════════════════════════════

def scrape_next_button(browser, base_url, next_sel, sels, skip_handles,
                       batch_size, output_path, review_path, limit, use_product_page):
    total_written = 0
    batch         = []
    first_flush   = not Path(output_path).exists()
    page_num      = 0
    catalog_page  = new_stealth_page(browser)
    product_page  = new_stealth_page(browser) if use_product_page else None

    print(f"\n[->] Opening {base_url}")
    catalog_page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)

    while True:
        page_num += 1
        print(f"\n{'=' * 60}")
        print(f"  PAGE {page_num}")
        print(f"{'=' * 60}")

        try:
            catalog_page.wait_for_selector(sels["card"], timeout=20_000)
        except PWTimeout:
            print(f"  [!] No cards on page {page_num} — stopping.")
            break

        batch, first_flush, total_written, hit_limit = extract_cards(
            catalog_page, product_page, base_url, sels, skip_handles,
            batch, first_flush, total_written, batch_size,
            output_path, review_path, limit, page_num, use_product_page,
        )
        if hit_limit:
            break

        next_btn = catalog_page.query_selector(next_sel)
        if not next_btn:
            print(f"\n[OK] Last page reached — no next button found.")
            break

        is_disabled = (
            next_btn.get_attribute("aria-disabled") == "true"
            or "disabled" in (next_btn.get_attribute("class") or "")
        )
        if is_disabled:
            print("\n[OK] Next button disabled — last page reached.")
            break

        print(f"\n  [nav] Going to next page...")
        next_btn.scroll_into_view_if_needed()
        next_btn.click()
        catalog_page.wait_for_load_state("domcontentloaded")
        time.sleep(1.5)

    if batch:
        flush_batch(batch, output_path, review_path, first_flush)
        total_written += len(batch)

    catalog_page.close()
    if product_page:
        product_page.close()
    return total_written

# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION — MODE 2: URL PATTERN
# ══════════════════════════════════════════════════════════════════════════════

def scrape_url_pattern(browser, url_pattern, page_start, page_end, sels, skip_handles,
                       batch_size, output_path, review_path, limit, use_product_page):
    total_written = 0
    batch         = []
    first_flush   = not Path(output_path).exists()
    catalog_page  = new_stealth_page(browser)
    product_page  = new_stealth_page(browser) if use_product_page else None

    for page_num in range(page_start, page_end + 1):
        url = url_pattern.format(page_num)
        print(f"\n{'=' * 60}")
        print(f"  PAGE {page_num} -> {url}")
        print(f"{'=' * 60}")

        try:
            catalog_page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            catalog_page.wait_for_selector(sels["card"], timeout=15_000)
        except PWTimeout:
            print(f"  [OK] No products on page {page_num} — catalog end reached.")
            break
        except Exception as e:
            print(f"  [!] Page {page_num} failed ({type(e).__name__}) — skipping.")
            time.sleep(2)
            continue

        count_before = total_written + len(batch)

        batch, first_flush, total_written, hit_limit = extract_cards(
            catalog_page, product_page, url, sels, skip_handles,
            batch, first_flush, total_written, batch_size,
            output_path, review_path, limit, page_num, use_product_page,
        )

        if (total_written + len(batch)) == count_before:
            print(f"  [OK] Page {page_num} has 0 new products — stopping.")
            break

        if hit_limit:
            break

        time.sleep(1.2)

    if batch:
        flush_batch(batch, output_path, review_path, first_flush)
        total_written += len(batch)

    catalog_page.close()
    if product_page:
        product_page.close()
    return total_written

# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION — MODE 3: SINGLE PAGE
# ══════════════════════════════════════════════════════════════════════════════

def scrape_single_page(browser, url, sels, skip_handles,
                       batch_size, output_path, review_path, limit, use_product_page):
    total_written = 0
    batch         = []
    first_flush   = not Path(output_path).exists()
    catalog_page  = new_stealth_page(browser)
    product_page  = new_stealth_page(browser) if use_product_page else None

    print(f"\n[->] Opening {url}")
    catalog_page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    try:
        catalog_page.wait_for_selector(sels["card"], timeout=20_000)
    except PWTimeout:
        print("\n[!] No product cards found.")
        print("    Fix: python \"real wold.py\" --card-sel \".your-product-class\"")
        catalog_page.close()
        return 0

    batch, first_flush, total_written, _ = extract_cards(
        catalog_page, product_page, url, sels, skip_handles,
        batch, first_flush, total_written, batch_size,
        output_path, review_path, limit, 1, use_product_page,
    )

    if batch:
        flush_batch(batch, output_path, review_path, first_flush)
        total_written += len(batch)

    catalog_page.close()
    if product_page:
        product_page.close()
    return total_written

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args        = parse_args()
    sels        = build_selectors(args)
    review_path = derive_review_path(args.output)
    use_pp      = not args.no_product_page

    skip_handles = already_scraped_handles(args.output) if args.resume else set()

    if not args.resume:
        for f in [args.output, review_path]:
            if Path(f).exists():
                Path(f).unlink()
                print(f"[init] Removed old file: {f}")

    print(f"\n{'=' * 60}")
    print(f"  UNIVERSAL E-COMMERCE SCRAPER")
    print(f"{'=' * 60}")
    print(f"  Product page scraping : {'ON' if use_pp else 'OFF'}")
    print(f"  Product limit         : {args.limit}")
    print(f"  Batch size            : {args.batch}")
    print(f"  Shopify CSV           : {args.output}")
    print(f"  Client Review CSV     : {review_path}")
    print(f"{'=' * 60}\n")

    common = dict(
        sels             = sels,
        skip_handles     = skip_handles,
        batch_size       = args.batch,
        output_path      = args.output,
        review_path      = review_path,
        limit            = args.limit,
        use_product_page = use_pp,
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless = True,
            args     = [
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )

        if args.url_pattern:
            print(f"[mode] URL PATTERN — pages {args.page_start} to {args.page_end}")
            written = scrape_url_pattern(
                browser, args.url_pattern,
                args.page_start, args.page_end,
                **common,
            )

        elif args.next_sel:
            base_url = load_target_url()
            print(f"[mode] NEXT BUTTON — '{args.next_sel}'")
            written = scrape_next_button(browser, base_url, args.next_sel, **common)

        else:
            base_url = load_target_url()
            print(f"[mode] SINGLE PAGE")
            written = scrape_single_page(browser, base_url, **common)

        browser.close()

    if written == 0:
        print(f"\n{'=' * 60}")
        print(f"  [X] NOTHING WRITTEN")
        print(f"  Check your selectors or target URL")
        print(f"{'=' * 60}")
        raise SystemExit(1)

    print(f"\n{'=' * 60}")
    print(f"  SCRAPING COMPLETE")
    print(f"  Total rows   : {written}")
    print(f"  Shopify file : {args.output}")
    print(f"  Client file  : {review_path}")
    print(f"{'=' * 60}")
