"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         ENTERPRISE SHOPIFY SCRAPER v3.0 — real wold.py                     ║
║         Now with: Product Pages + Variants + Multiple Images                ║
║                                                                              ║
║  HOW TO RUN:                                                                 ║
║                                                                              ║
║  1. BASIC (reads URL from target.txt):                                       ║
║       python "real wold.py"                                                  ║
║                                                                              ║
║  2. CUSTOM SELECTORS:                                                        ║
║       python "real wold.py" --title-sel "h1.product-title"                  ║
║                             --price-sel ".price-tag"                         ║
║                             --link-sel  "a.product-link"                     ║
║                                                                              ║
║  3. SKIP PRODUCT PAGES (faster, less data):                                  ║
║       python "real wold.py" --no-product-page                               ║
║                                                                              ║
║  4. NEXT BUTTON PAGINATION:                                                  ║
║       python "real wold.py" --next-sel "a.pagination__next"                 ║
║                                                                              ║
║  5. URL PATTERN PAGINATION:                                                  ║
║       python "real wold.py" --url-pattern "https://site.com/shop?page={}"  ║
║                             --page-start 1 --page-end 50                    ║
║                                                                              ║
║  6. RESUME A CRASHED RUN:                                                    ║
║       python "real wold.py" --resume                                         ║
║                                                                              ║
║  7. FULL ENTERPRISE RUN:                                                     ║
║       python "real wold.py" --next-sel "a.pagination__next"                 ║
║                             --title-sel "h1.product__title"                  ║
║                             --price-sel ".price__regular"                    ║
║                             --limit 500 --batch 20                           ║
║                             --output client_export.csv                       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

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

# Exact Shopify-required headers — never change spelling or casing
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

# Human-readable headers for client review file
CLIENT_COLUMNS = [
    "Product Handle (URL Slug)",
    "Product Title",
    "Description (HTML)",
    "Option Name (e.g. Size / Color)",
    "Option Value (e.g. Small / Red)",
    "Variant SKU",
    "Price",
    "Image URL (Click to Preview)",
]


# ══════════════════════════════════════════════════════════════════════════════
# DEFAULTS
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
    # ── Card level (catalog/collection page) ──────────────────────────────────
    "card":       (
        "[class*='product-card'], [class*='product-item'], "
        "[class*='ProductCard'], article[data-product], li[data-product-id]"
    ),
    "link":       "a",                          # first link inside card → product URL

    # ── Product page level ────────────────────────────────────────────────────
    "title":      "h1, [class*='product-title'], [class*='ProductName']",
    "price":      "[class*='price']:not([class*='compare']), [data-price]",
    "desc":       (
        "[class*='product-description'], [class*='description__text'], "
        "#product-description, [class*='product-detail'] p"
    ),
    "main_img":   (
        "[class*='product-image'] img, [class*='ProductImage'] img, "
        ".product__media img, [data-product-image] img"
    ),
    "extra_imgs": (
        "[class*='product-image'] img, [class*='thumbnail'] img, "
        "[class*='gallery'] img"
    ),
    # Variant selector — tries Shopify JSON first, then falls back to this
    "variants":   "select[name*='option'] option, [class*='variant'] input[type='radio']",
}


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Enterprise Shopify Scraper v3 — Product Pages + Variants + Multi-Image",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Run control
    p.add_argument("--limit",           type=int,  default=PRODUCT_LIMIT)
    p.add_argument("--batch",           type=int,  default=BATCH_SIZE)
    p.add_argument("--output",          default=OUTPUT_FILE)
    p.add_argument("--resume",          action="store_true")
    p.add_argument("--no-product-page", action="store_true",
                   help="Skip opening product pages — faster but less data")

    # Card-level selector overrides
    p.add_argument("--card-sel",  default=None, help="Product card container")
    p.add_argument("--link-sel",  default=None, help="Product page link inside card")

    # Product page selector overrides
    p.add_argument("--title-sel",      default=None, help="Product title on product page")
    p.add_argument("--price-sel",      default=None, help="Product price on product page")
    p.add_argument("--desc-sel",       default=None, help="Full description on product page")
    p.add_argument("--main-img-sel",   default=None, help="Main product image")
    p.add_argument("--extra-imgs-sel", default=None, help="Additional product images")
    p.add_argument("--variant-sel",    default=None, help="Variant options selector")

    # Pagination
    p.add_argument("--next-sel",    default=None)
    p.add_argument("--url-pattern", default=None)
    p.add_argument("--page-start",  type=int, default=1)
    p.add_argument("--page-end",    type=int, default=50)

    return p.parse_args()


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def load_target_url() -> str:
    target_path = Path(__file__).parent / TARGET_FILE
    if not target_path.exists():
        print(f"[✗] '{TARGET_FILE}' not found. Create it and paste your URL inside.")
        raise SystemExit(1)
    url = target_path.read_text(encoding="utf-8").strip()
    if not url or not url.startswith("http"):
        print(f"[✗] Invalid URL in target.txt: '{url}'")
        raise SystemExit(1)
    print(f"[✓] Base URL loaded: {url}")
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
    return re.sub(r"[^\d.]", "", raw).strip() or "0.00"


def resolve_url(href: str, base_url: str) -> str:
    """Converts relative URLs to absolute."""
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
    if url and url.startswith("//"):
        return "https:" + url
    return url or ""


def already_scraped_handles(output_path: str) -> set:
    p = Path(output_path)
    if not p.exists():
        return set()
    try:
        existing = pd.read_csv(p, usecols=["Handle"])
        handles  = set(existing["Handle"].dropna().tolist())
        print(f"[resume] {len(handles)} handles already saved — skipping.")
        return handles
    except Exception as e:
        print(f"[resume] Could not read file ({e}) — starting fresh.")
        return set()


def derive_review_path(output_path: str) -> str:
    stem = Path(output_path).stem
    return str(Path(output_path).parent / f"{stem}_client_review.csv")


# ══════════════════════════════════════════════════════════════════════════════
# GEMINI — bulletproof, never crashes the pipeline
# ══════════════════════════════════════════════════════════════════════════════
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
                print(f"    [Gemini] All retries failed — raw text fallback.")
                safe = raw_text.replace("\n\n", "</p><p>").replace("\n", " ")
                return f"<p>{safe}</p>", False

    return f"<p>{raw_text}</p>", False


# ══════════════════════════════════════════════════════════════════════════════
# FIX 2: VARIANTS EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════
def extract_variants(page, base_price: str, sels: dict) -> list[dict]:
    """
    Tries 3 methods to extract variants:
    1. Shopify product JSON (most reliable — works on all Shopify stores)
    2. WooCommerce variation data
    3. DOM fallback (select/radio elements)

    Returns list of dicts: [{name, value, sku, price}]
    If no variants found, returns single default row.
    """
    variants = []

    # ── Method 1: Shopify product JSON in window object ───────────────────────
    try:
        product_data = page.evaluate("""
            () => {
                // Try ShopifyAnalytics
                if (window.ShopifyAnalytics && window.ShopifyAnalytics.meta &&
                    window.ShopifyAnalytics.meta.product) {
                    return window.ShopifyAnalytics.meta.product;
                }
                // Try __st (Shopify tracking)
                if (window.__st && window.__st.p) {
                    return window.__st.p;
                }
                // Try application/json script tags
                const scripts = document.querySelectorAll(
                    'script[type="application/json"], script[id*="product"]'
                );
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
                price = str(int(v.get("price", 0)) / 100) if v.get("price") else base_price
                variants.append({
                    "name":  option_name,
                    "value": v.get("option1") or v.get("title") or "Default",
                    "sku":   v.get("sku", ""),
                    "price": price,
                })

            if variants:
                print(f"    [variants] Found {len(variants)} via Shopify JSON ✓")
                return variants

    except Exception:
        pass

    # ── Method 2: WooCommerce variation data ──────────────────────────────────
    try:
        woo_data = page.evaluate("""
            () => {
                const form = document.querySelector('form.variations_form');
                if (!form) return null;
                const data = form.getAttribute('data-product_variations');
                return data ? JSON.parse(data) : null;
            }
        """)

        if woo_data and isinstance(woo_data, list):
            for v in woo_data:
                attrs = v.get("attributes", {})
                attr_val = list(attrs.values())[0] if attrs else "Default"
                attr_name = list(attrs.keys())[0].replace("attribute_pa_", "").title() if attrs else "Option"
                price = v.get("display_price", base_price)
                variants.append({
                    "name":  attr_name,
                    "value": attr_val,
                    "sku":   v.get("sku", ""),
                    "price": str(price),
                })

            if variants:
                print(f"    [variants] Found {len(variants)} via WooCommerce JSON ✓")
                return variants

    except Exception:
        pass

    # ── Method 3: DOM fallback — select/radio elements ────────────────────────
    try:
        option_els = page.query_selector_all(sels["variants"])
        seen = set()
        for el in option_els:
            value = (el.get_attribute("value") or el.inner_text()).strip()
            if value and value.lower() not in ("", "choose", "select", "pick"):
                if value not in seen:
                    seen.add(value)
                    variants.append({
                        "name":  "Option1",
                        "value": value,
                        "sku":   "",
                        "price": base_price,
                    })

        if variants:
            print(f"    [variants] Found {len(variants)} via DOM fallback ✓")
            return variants

    except Exception:
        pass

    # ── No variants found — return Shopify default single variant ─────────────
    return [{
        "name":  "Title",
        "value": "Default Title",
        "sku":   "",
        "price": base_price,
    }]


# ══════════════════════════════════════════════════════════════════════════════
# FIX 3: MULTIPLE IMAGE EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════
def extract_images(page, sels: dict) -> tuple[str, list[str]]:
    """
    Returns (main_image_url, [extra_image_url1, extra_image_url2, ...])
    Deduplicates and filters out placeholder/icon images.
    """
    all_images = []

    try:
        img_els = page.query_selector_all(sels["extra_imgs"])
        for el in img_els:
            src = (
                el.get_attribute("src")
                or el.get_attribute("data-src")
                or el.get_attribute("data-zoom-src")
                or el.get_attribute("data-large-src")
                or ""
            )
            src = clean_image_url(src)

            # Filter out tiny icons and placeholders
            if not src:
                continue
            if any(x in src.lower() for x in ["placeholder", "blank", "loading", "svg+xml"]):
                continue
            width = el.get_attribute("width")
            if width and int(width) < 100:
                continue

            if src not in all_images:
                all_images.append(src)

    except Exception:
        pass

    # Try main image selector if no images found yet
    if not all_images:
        try:
            main_el = page.query_selector(sels["main_img"])
            if main_el:
                src = clean_image_url(
                    main_el.get_attribute("src")
                    or main_el.get_attribute("data-src")
                    or ""
                )
                if src:
                    all_images.append(src)
        except Exception:
            pass

    main_image   = all_images[0] if all_images else ""
    extra_images = all_images[1:] if len(all_images) > 1 else []

    return main_image, extra_images


# ══════════════════════════════════════════════════════════════════════════════
# FIX 1: PRODUCT PAGE SCRAPER
# ══════════════════════════════════════════════════════════════════════════════
def scrape_product_page(product_page, url: str, sels: dict) -> dict:
    """
    Navigates to a product page and extracts:
    - Full description (not the short card snippet)
    - High quality main image
    - Additional images
    - Variants (size, color, SKU, price)

    Uses a shared page object to avoid memory overhead.
    """
    try:
        product_page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(0.8)  # small pause for JS to render

        # ── Title ──────────────────────────────────────────────────────────────
        title_el = product_page.query_selector(sels["title"])
        title    = title_el.inner_text().strip() if title_el else ""

        # ── Price ──────────────────────────────────────────────────────────────
        price_el = product_page.query_selector(sels["price"])
        price    = clean_price(price_el.inner_text()) if price_el else "0.00"

        # ── Full Description ───────────────────────────────────────────────────
        desc_el  = product_page.query_selector(sels["desc"])
        raw_desc = desc_el.inner_text().strip() if desc_el else ""

        # ── Images ────────────────────────────────────────────────────────────
        main_image, extra_images = extract_images(product_page, sels)

        # ── Variants ──────────────────────────────────────────────────────────
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
# ROW BUILDER — handles variants + multiple images for Shopify CSV format
# ══════════════════════════════════════════════════════════════════════════════
def build_product_rows(
    handle:       str,
    title:        str,
    body_html:    str,
    price:        str,
    main_image:   str,
    extra_images: list[str],
    variants:     list[dict],
) -> list[dict]:
    """
    Builds multiple CSV rows per product following exact Shopify import rules:

    Row 1  → full data: title, body, first variant, main image
    Row 2+ → same handle, empty title/body, next variant or extra image

    Shopify matches rows by Handle — all rows with same handle = same product.
    """
    rows    = []
    is_first = True

    def empty_row(handle: str) -> dict:
        return {col: "" for col in SHOPIFY_COLUMNS} | {"Handle": handle}

    # ── Write one row per variant ─────────────────────────────────────────────
    for i, variant in enumerate(variants):
        row = empty_row(handle)
        row["Option1 Name"]  = variant["name"]
        row["Option1 Value"] = variant["value"]
        row["Variant SKU"]   = variant["sku"]
        row["Variant Price"] = variant["price"] or price

        if is_first:
            # First row carries all the product-level data
            row["Title"]      = title
            row["Body (HTML)"] = body_html
            row["Image Src"]  = main_image
            is_first = False
        else:
            # Additional variant rows get extra images if available
            img_index = i - 1
            if img_index < len(extra_images):
                row["Image Src"] = extra_images[img_index]

        rows.append(row)

    # ── Write remaining extra images (beyond variant count) ───────────────────
    used_img_count = max(0, len(variants) - 1)   # images used in variant rows
    for img in extra_images[used_img_count:]:
        row = empty_row(handle)
        row["Image Src"] = img
        rows.append(row)

    return rows


# ══════════════════════════════════════════════════════════════════════════════
# BATCH WRITER — writes BOTH CSV files simultaneously (crash-safe)
# ══════════════════════════════════════════════════════════════════════════════
def flush_batch(
    batch:       list[dict],
    output_path: str,
    review_path: str,
    first_flush: bool,
) -> None:
    if not batch:
        return

    # ── Shopify import CSV ─────────────────────────────────────────────────────
    df_shopify   = pd.DataFrame(batch, columns=SHOPIFY_COLUMNS)
    write_header = first_flush and not Path(output_path).exists()
    df_shopify.to_csv(output_path, mode="a", index=False,
                      header=write_header, encoding="utf-8-sig")

    # ── Client review CSV (readable names + clickable image links) ────────────
    df_client         = df_shopify.copy()
    df_client.columns = CLIENT_COLUMNS
    df_client["Image URL (Click to Preview)"] = df_client[
        "Image URL (Click to Preview)"
    ].apply(
        lambda u: f'=HYPERLINK("{u}","View Image")' if isinstance(u, str) and u.startswith("http") else u
    )

    write_header_client = first_flush and not Path(review_path).exists()
    df_client.to_csv(review_path, mode="a", index=False,
                     header=write_header_client, encoding="utf-8-sig")

    # Count unique products (not rows — variants create multiple rows)
    unique_handles = df_shopify["Handle"].nunique()
    print(f"  [disk] ✓ {unique_handles} products ({len(batch)} rows) saved")
    print(f"         Shopify  → {output_path}")
    print(f"         Client   → {review_path}")


# ══════════════════════════════════════════════════════════════════════════════
# CARD EXTRACTOR — shared core used by all 3 pagination engines
# ══════════════════════════════════════════════════════════════════════════════
def extract_cards(
    catalog_page,
    product_page,
    base_url:     str,
    sels:         dict,
    skip_handles: set,
    batch:        list,
    first_flush:  bool,
    total_written: int,
    batch_size:   int,
    output_path:  str,
    review_path:  str,
    limit:        int,
    page_num:     int,
    use_product_page: bool,
) -> tuple[list, bool, int, bool]:

    # Scroll to trigger lazy loading
    for _ in range(4):
        catalog_page.evaluate("window.scrollBy(0, window.innerHeight)")
        time.sleep(0.5)

    cards = catalog_page.query_selector_all(sels["card"])
    print(f"  [page {page_num}] {len(cards)} product cards detected")

    for card in cards:
        if total_written + len(batch) >= limit:
            print(f"  [limit] Reached {limit} products — stopping.")
            return batch, first_flush, total_written, True

        # ── Step 1: Extract from card ──────────────────────────────────────────
        try:
            # Card-level title/price as fallback
            card_title_el = card.query_selector(sels["title"])
            card_title    = card_title_el.inner_text().strip() if card_title_el else ""

            card_price_el = card.query_selector(sels["price"])
            card_price    = clean_price(card_price_el.inner_text()) if card_price_el else "0.00"

            card_img_el   = card.query_selector("img")
            card_image    = ""
            if card_img_el:
                card_image = clean_image_url(
                    card_img_el.get_attribute("src")
                    or card_img_el.get_attribute("data-src")
                    or ""
                )

            card_desc_el  = card.query_selector(sels["desc"])
            card_desc     = card_desc_el.inner_text().strip() if card_desc_el else ""

            # Get product page link
            link_el  = card.query_selector(sels["link"])
            href     = link_el.get_attribute("href") if link_el else ""
            full_url = resolve_url(href, base_url)

        except Exception as e:
            print(f"    [!] Card DOM error ({type(e).__name__}) — skipping.")
            continue

        # Use card title for handle check
        title  = card_title or "Untitled Product"
        handle = slugify(title)

        if handle in skip_handles:
            print(f"    [skip] Already saved: {title}")
            continue

        # ── Step 2: Open product page for full data ────────────────────────────
        page_data = {}
        if use_product_page and full_url:
            print(f"  [→] Opening product page: {full_url}")
            page_data = scrape_product_page(product_page, full_url, sels)

        # ── Step 3: Merge card data + product page data ────────────────────────
        # Product page data wins — card data is fallback
        final_title  = page_data.get("title")  or card_title  or "Untitled Product"
        final_price  = page_data.get("price")  or card_price
        final_desc   = page_data.get("raw_desc") or card_desc
        main_image   = page_data.get("main_image") or card_image
        extra_images = page_data.get("extra_images", [])
        variants     = page_data.get("variants") or [{
            "name": "Title", "value": "Default Title",
            "sku": "", "price": final_price
        }]

        handle = slugify(final_title)

        # ── Step 4: Gemini format the description ──────────────────────────────
        running_total = total_written + len(batch) + 1
        print(f"\n  [{running_total:04d}] {final_title}")
        print(f"         Price: {final_price}  |  "
              f"Images: {1 + len(extra_images)}  |  "
              f"Variants: {len(variants)}")

        body_html, used_ai = gemini_format(final_desc)
        print(f"         Description: [{'AI ✓' if used_ai else 'RAW FALLBACK'}]")

        # ── Step 5: Build rows (variants × images) ─────────────────────────────
        product_rows = build_product_rows(
            handle       = handle,
            title        = final_title,
            body_html    = body_html,
            price        = final_price,
            main_image   = main_image,
            extra_images = extra_images,
            variants     = variants,
        )

        batch.extend(product_rows)
        skip_handles.add(handle)
        print(f"         → {len(product_rows)} rows added to batch")

        # ── Flush when batch is full ───────────────────────────────────────────
        if len(batch) >= batch_size:
            flush_batch(batch, output_path, review_path, first_flush)
            total_written += len(batch)
            first_flush    = False
            batch          = []

    return batch, first_flush, total_written, False


# ══════════════════════════════════════════════════════════════════════════════
# BROWSER PAGE FACTORY
# ══════════════════════════════════════════════════════════════════════════════
def new_stealth_page(browser):
    """Creates a new stealth-configured browser page."""
    page = browser.new_page(
        user_agent       = STEALTH_UA,
        viewport         = STEALTH_VIEWPORT,
        extra_http_headers = STEALTH_HEADERS,
    )
    page.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    return page


# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION ENGINE — MODE 1: NEXT BUTTON
# ══════════════════════════════════════════════════════════════════════════════
def scrape_next_button(
    browser, base_url: str, next_sel: str,
    sels: dict, skip_handles: set,
    batch_size: int, output_path: str,
    review_path: str, limit: int,
    use_product_page: bool,
) -> int:
    total_written = 0
    batch: list[dict] = []
    first_flush = not Path(output_path).exists()
    page_num    = 0

    # Two pages: one for catalog browsing, one dedicated for product pages
    catalog_page = new_stealth_page(browser)
    product_page = new_stealth_page(browser) if use_product_page else None

    print(f"\n[→] Opening {base_url}")
    catalog_page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)

    while True:
        page_num += 1
        print(f"\n── Page {page_num} {'─' * 55}")

        try:
            catalog_page.wait_for_selector(sels["card"], timeout=20_000)
        except PWTimeout:
            print(f"  [!] No product cards on page {page_num} — stopping.")
            break

        batch, first_flush, total_written, hit_limit = extract_cards(
            catalog_page, product_page, base_url,
            sels, skip_handles, batch, first_flush,
            total_written, batch_size, output_path, review_path,
            limit, page_num, use_product_page,
        )
        if hit_limit:
            break

        next_btn = catalog_page.query_selector(next_sel)
        if not next_btn:
            print(f"\n[✓] No next button found — reached last page.")
            break

        is_disabled = (
            next_btn.get_attribute("aria-disabled") == "true"
            or "disabled" in (next_btn.get_attribute("class") or "")
        )
        if is_disabled:
            print("\n[✓] Next button disabled — reached last page.")
            break

        print(f"\n  [nav] Clicking next page…")
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
# PAGINATION ENGINE — MODE 2: URL PATTERN
# ══════════════════════════════════════════════════════════════════════════════
def scrape_url_pattern(
    browser, url_pattern: str,
    page_start: int, page_end: int,
    sels: dict, skip_handles: set,
    batch_size: int, output_path: str,
    review_path: str, limit: int,
    use_product_page: bool,
) -> int:
    total_written = 0
    batch: list[dict] = []
    first_flush = not Path(output_path).exists()

    catalog_page = new_stealth_page(browser)
    product_page = new_stealth_page(browser) if use_product_page else None

    for page_num in range(page_start, page_end + 1):
        url = url_pattern.format(page_num)
        print(f"\n── Page {page_num} → {url} {'─' * 30}")

        try:
            catalog_page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            catalog_page.wait_for_selector(sels["card"], timeout=15_000)
        except PWTimeout:
            print(f"  [✓] No products on page {page_num} — catalog end.")
            break
        except Exception as e:
            print(f"  [!] Failed page {page_num} ({type(e).__name__}) — skipping.")
            time.sleep(2)
            continue

        prev_count = total_written + len(batch)

        batch, first_flush, total_written, hit_limit = extract_cards(
            catalog_page, product_page, url,
            sels, skip_handles, batch, first_flush,
            total_written, batch_size, output_path, review_path,
            limit, page_num, use_product_page,
        )

        if (total_written + len(batch)) == prev_count:
            print(f"  [✓] Page {page_num} returned 0 new products — stopping.")
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
# PAGINATION ENGINE — MODE 3: SINGLE PAGE
# ══════════════════════════════════════════════════════════════════════════════
def scrape_single_page(
    browser, url: str,
    sels: dict, skip_handles: set,
    batch_size: int, output_path: str,
    review_path: str, limit: int,
    use_product_page: bool,
) -> int:
    total_written = 0
    batch: list[dict] = []
    first_flush = not Path(output_path).exists()

    catalog_page = new_stealth_page(browser)
    product_page = new_stealth_page(browser) if use_product_page else None

    print(f"[→] Opening {url}")
    catalog_page.goto(url, wait_until="domcontentloaded", timeout=60_000)

    try:
        catalog_page.wait_for_selector(sels["card"], timeout=20_000)
    except PWTimeout:
        print("[!] No product cards found. Try --card-sel with a custom selector.")
        catalog_page.close()
        return 0

    batch, first_flush, total_written, _ = extract_cards(
        catalog_page, product_page, url,
        sels, skip_handles, batch, first_flush,
        total_written, batch_size, output_path, review_path,
        limit, 1, use_product_page,
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
    use_pp      = not args.no_product_page   # True by default

    skip_handles = already_scraped_handles(args.output) if args.resume else set()

    if not args.resume:
        for f in [args.output, review_path]:
            if Path(f).exists():
                Path(f).unlink()
                print(f"[init] Removed old file: {f}")

    print("\n[config] Active selectors:")
    for k, v in sels.items():
        print(f"  {k:12s} → {v[:60]}{'…' if len(v) > 60 else ''}")

    print(f"\n[config] Product page scraping: {'ON ✓' if use_pp else 'OFF (--no-product-page)'}")
    print(f"[output] Shopify CSV   → {args.output}")
    print(f"[output] Client Review → {review_path}\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )

        common = dict(
            sels         = sels,
            skip_handles = skip_handles,
            batch_size   = args.batch,
            output_path  = args.output,
            review_path  = review_path,
            limit        = args.limit,
            use_product_page = use_pp,
        )

        if args.url_pattern:
            print(f"[mode] URL Pattern — pages {args.page_start} to {args.page_end}\n")
            written = scrape_url_pattern(
                browser, args.url_pattern,
                args.page_start, args.page_end, **common
            )

        elif args.next_sel:
            base_url = load_target_url()
            print(f"[mode] Next-Button — selector: '{args.next_sel}'\n")
            written = scrape_next_button(browser, base_url, args.next_sel, **common)

        else:
            base_url = load_target_url()
            print(f"[mode] Single Page\n")
            written = scrape_single_page(browser, base_url, **common)

        browser.close()

    if written == 0:
        print("\n[✗] Nothing written. Check selectors or target URL.")
        raise SystemExit(1)

    print(f"\n{'═' * 60}")
    print(f"  [✓] COMPLETE — {written} total rows saved")
    print(f"  [✓] Upload to Shopify  → {args.output}")
    print(f"  [✓] Send to client     → {review_path}")
    print(f"{'═' * 60}")