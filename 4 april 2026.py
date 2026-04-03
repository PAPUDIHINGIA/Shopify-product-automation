import os
import re
import sys
import time
import random
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urljoin
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.environ["GEMINI_API_KEY"])


# ══════════════════════════════════════════════════════════════════════════════
# RUN STATS — tracks progress, AI success rate, failures across the whole run
# ══════════════════════════════════════════════════════════════════════════════

class RunStats:
    """
    Single shared object passed through the pipeline.
    Tracks everything so the final summary is meaningful.
    """
    def __init__(self, limit: int):
        self.limit          = limit
        self.total_products = 0       # unique products processed
        self.total_rows     = 0       # CSV rows written (variants × images)
        self.ai_success     = 0       # times Gemini formatted OK
        self.ai_fallback    = 0       # times raw text fallback was used
        self.page_failures  = 0       # product pages that failed to load
        self.failed_urls: list[str] = []  # saved for retry / reporting
        self.started_at     = datetime.now()

    def record_product(self, rows: int, used_ai: bool, failed_url: str = ""):
        self.total_products += 1
        self.total_rows     += rows
        if used_ai:
            self.ai_success += 1
        else:
            self.ai_fallback += 1
        if failed_url:
            self.page_failures += 1
            self.failed_urls.append(failed_url)

    def progress_line(self) -> str:
        pct = min(100, int(self.total_products / self.limit * 100))
        bar = ("█" * (pct // 5)).ljust(20)
        return f"  [{bar}] {pct}%  ({self.total_products}/{self.limit})"

    def summary(self) -> str:
        elapsed = datetime.now() - self.started_at
        mins, secs = divmod(int(elapsed.total_seconds()), 60)
        ai_rate = (
            int(self.ai_success / (self.ai_success + self.ai_fallback) * 100)
            if (self.ai_success + self.ai_fallback) > 0 else 0
        )
        lines = [
            f"\n{'=' * 60}",
            f"  SCRAPING COMPLETE",
            f"{'=' * 60}",
            f"  Runtime          : {mins}m {secs}s",
            f"  Products scraped : {self.total_products}",
            f"  CSV rows written : {self.total_rows}",
            f"  Gemini AI rate   : {ai_rate}% ({self.ai_success} OK / {self.ai_fallback} fallback)",
            f"  Page failures    : {self.page_failures}",
        ]
        if self.failed_urls:
            lines.append(f"  Failed URLs      : saved to failed_urls.txt")
        lines.append(f"{'=' * 60}")
        return "\n".join(lines)

    def save_failed_urls(self, output_dir: str = "."):
        if not self.failed_urls:
            return
        path = Path(output_dir) / "failed_urls.txt"
        path.write_text("\n".join(self.failed_urls), encoding="utf-8")
        print(f"  [!] {len(self.failed_urls)} failed URLs saved → {path}")

# ══════════════════════════════════════════════════════════════════════════════
# COLUMN DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

SHOPIFY_COLUMNS = [
    "Handle",
    "Title",
    "Body (HTML)",
    # PATCH 1: 3 fully-separated option columns — Shopify requires these distinct,
    # never merged. Merging "Small / Red" into Option1 Value breaks variant matching.
    "Option1 Name",
    "Option1 Value",
    "Option2 Name",
    "Option2 Value",
    "Option3 Name",
    "Option3 Value",
    "Variant SKU",
    "Variant Price",
    "Image Src",
    # FEATURE: store raw URL so flush_batch can wrap it in HYPERLINK formula
    "Source URL",
]

CLIENT_COLUMNS = [
    "Product Handle (URL Slug)",
    "Product Title",
    "Full Description (HTML)",
    "Option 1 Name",
    "Option 1 Value",
    "Option 2 Name",
    "Option 2 Value",
    "Option 3 Name",
    "Option 3 Value (or overflow)",
    "Variant SKU Code",
    "Price",
    "Image URL (Click to Preview)",
    # FEATURE: clickable link to original supplier product page
    "Original Source Link",
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
    p = argparse.ArgumentParser(description="Universal E-Commerce Scraper -> Shopify CSV")
    p.add_argument("--limit",           type=int,   default=PRODUCT_LIMIT)
    p.add_argument("--batch",           type=int,   default=BATCH_SIZE)
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
    p.add_argument("--page-start",      type=int,   default=1)
    p.add_argument("--page-end",        type=int,   default=50)
    # ── NEW: production control args ──────────────────────────────────────────
    p.add_argument("--delay",           type=float, default=1.0,
                   help="Seconds between product page requests (default: 1.0)")
    p.add_argument("--show-browser",    action="store_true",
                   help="Show browser window — useful for debugging selectors")
    p.add_argument("--log-file",        default="scraper.log",
                   help="Save all output to this log file (default: scraper.log)")
    p.add_argument("--max-retries",     type=int,   default=2,
                   help="Retries per failed product page (default: 2)")
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
    try:
        return urljoin(base_url, href)
    except Exception:
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


# ── NEW: file logger — mirrors everything printed to terminal into a log file ──
def setup_logging(log_file: str) -> None:
    """
    Tees all print() output to both terminal AND a log file simultaneously.
    This means every run creates a full audit trail automatically.
    Check scraper.log after a run to see exactly what happened per product.
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    class TeeWriter:
        def __init__(self, *writers):
            self.writers = writers
        def write(self, text):
            for w in self.writers:
                try:
                    w.write(text)
                    w.flush()
                except Exception:
                    pass
        def flush(self):
            for w in self.writers:
                try:
                    w.flush()
                except Exception:
                    pass

    log_handle = open(log_path, "a", encoding="utf-8")
    log_handle.write(f"\n{'=' * 60}\n")
    log_handle.write(f"  RUN STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    log_handle.write(f"{'=' * 60}\n")
    sys.stdout = TeeWriter(sys.__stdout__, log_handle)
    print(f"[log] Output mirrored to: {log_path}")


def new_stealth_page(context):
    """
    PATCH 3: accepts a persistent BrowserContext instead of browser.
    All stealth headers/UA/viewport are already baked into the context
    at startup — no need to set them per-page. This means one shared
    context, not a new context object per product page.
    """
    page = context.new_page()
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
    """
    PATCH 1 — returns proper per-axis option dicts.

    Each returned dict now carries up to 3 separate option axes:
        {
            "opt1_name": "Size",   "opt1_value": "Small",
            "opt2_name": "Color",  "opt2_value": "Red",
            "opt3_name": "",       "opt3_value": "",
            "sku":  "SKU-001",
            "price": "29.99",
        }

    Failsafe rule: if Shopify JSON returns more than 3 option axes,
    axes 3-N are concatenated with " / " and placed into opt3_value.

    Priority:
    1. Shopify product JSON  — has per-variant price + SKU
    2. WooCommerce JSON      — for WooCommerce stores
    3. DOM fallback          — generic selects/radio inputs
    """
    variants = []

    def _make_variant(opt1_name="", opt1_val="", opt2_name="", opt2_val="",
                      opt3_name="", opt3_val="", sku="", price=""):
        return {
            "opt1_name":  opt1_name,
            "opt1_value": opt1_val,
            "opt2_name":  opt2_name,
            "opt2_value": opt2_val,
            "opt3_name":  opt3_name,
            "opt3_value": opt3_val,
            "sku":        sku,
            "price":      price or base_price,
        }

    # ── Method 1: Shopify product JSON ────────────────────────────────────────
    try:
        product_data = page.evaluate("""
            () => {
                if (window.ShopifyAnalytics && window.ShopifyAnalytics.meta &&
                    window.ShopifyAnalytics.meta.product) {
                    return window.ShopifyAnalytics.meta.product;
                }
                if (window.__st && window.__st.p) { return window.__st.p; }
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
            # options is an array of option-axis names, e.g. ["Size","Color","Material"]

            for v in raw_variants:
                raw_p = v.get("price", 0)
                price = f"{int(raw_p) / 100:.2f}" if raw_p else base_price

                opt_vals = [
                    v.get("option1", ""),
                    v.get("option2", ""),
                    v.get("option3", ""),
                ]
                opt_names = options + [""] * 3   # pad to at least 3

                # FAILSAFE: if source has >3 axes, overflow into opt3_value
                if len(options) > 3:
                    overflow_vals  = [v.get(f"option{i}", "") for i in range(4, len(options) + 1)]
                    overflow_names = options[3:]
                    opt_vals[2]    = " / ".join(
                        f"{n}:{val}" for n, val in zip(overflow_names, overflow_vals) if val
                    )
                    opt_names[2] = "Options"

                variants.append(_make_variant(
                    opt1_name = opt_names[0],
                    opt1_val  = opt_vals[0],
                    opt2_name = opt_names[1] if len(opt_vals) > 1 else "",
                    opt2_val  = opt_vals[1]  if len(opt_vals) > 1 else "",
                    opt3_name = opt_names[2] if len(opt_vals) > 2 else "",
                    opt3_val  = opt_vals[2]  if len(opt_vals) > 2 else "",
                    sku       = v.get("sku", ""),
                    price     = price,
                ))

            if variants:
                print(f"    [variants] {len(variants)} found via Shopify JSON")
                return variants

    except Exception:
        pass

    # ── Method 2: WooCommerce JSON ────────────────────────────────────────────
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
                attr_vals = list(attrs.values())

                def _woo_name(key):
                    return key.replace("attribute_pa_", "").replace("attribute_", "").title()

                # Map up to 3 WooCommerce attribute axes
                variants.append(_make_variant(
                    opt1_name = _woo_name(attr_keys[0]) if len(attr_keys) > 0 else "Option",
                    opt1_val  = str(attr_vals[0])       if len(attr_vals) > 0 else "",
                    opt2_name = _woo_name(attr_keys[1]) if len(attr_keys) > 1 else "",
                    opt2_val  = str(attr_vals[1])       if len(attr_vals) > 1 else "",
                    # FAILSAFE: concatenate overflow axes into opt3
                    opt3_name = "Options"               if len(attr_keys) > 2 else "",
                    opt3_val  = " / ".join(
                        f"{_woo_name(k)}:{attr_vals[i]}"
                        for i, k in enumerate(attr_keys[2:], start=2)
                        if i < len(attr_vals)
                    ) if len(attr_keys) > 2 else "",
                    sku       = v.get("sku", ""),
                    price     = str(v.get("display_price", base_price)),
                ))
            if variants:
                print(f"    [variants] {len(variants)} found via WooCommerce JSON")
                return variants
    except Exception:
        pass

    # ── Method 3: DOM fallback ────────────────────────────────────────────────
    try:
        option_els = page.query_selector_all(sels["variants"])
        seen = set()
        for el in option_els:
            value = (el.get_attribute("value") or el.inner_text()).strip()
            skip_phrases = (
                "", "choose", "select", "pick",
                "choose an option", "select an option", "please select"
            )
            if not value or value.lower() in skip_phrases:
                continue
            if value not in seen:
                seen.add(value)
                variants.append(_make_variant(
                    opt1_name = "Option1",
                    opt1_val  = value,
                    price     = base_price,
                ))
        if variants:
            print(f"    [variants] {len(variants)} found via DOM fallback")
            return variants
    except Exception:
        pass

    print(f"    [variants] None found — using default single variant")
    return [_make_variant(opt1_name="Title", opt1_val="Default Title", price=base_price)]

# ══════════════════════════════════════════════════════════════════════════════
# IMAGE EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

def extract_images(page, sels: dict, base_url: str | None = None) -> tuple[str, list[str]]:
    """
    UPGRADED: now recovers full-resolution Shopify CDN images.

    Shopify CDN URLs often contain size suffixes like _1024x or _600x600.
    Removing these suffixes returns the original full-resolution image.
    Example:
      cdn.shopify.com/s/files/.../shirt_1024x.jpg
      → cdn.shopify.com/s/files/.../shirt.jpg  (original res)

    Also handles srcset attributes which contain the highest quality URLs.
    """
    all_images = []
    page_url   = base_url or getattr(page, "url", "")

    def upgrade_shopify_cdn(url: str) -> str:
        """Strip Shopify CDN size suffix to get full-resolution image."""
        if "cdn.shopify.com" in url or "shopify.com" in url:
            # Remove dimension suffixes like _1024x1024, _600x, _x600
            url = re.sub(r"_\d+x\d*(?=\.[a-z]{3,4}(\?|$))", "", url)
            url = re.sub(r"_x\d+(?=\.[a-z]{3,4}(\?|$))",    "", url)
        return url

    def best_src_from_element(el) -> str:
        """
        Pick the highest quality source from an img element.
        Checks srcset first (contains multiple resolutions), then data attrs.
        """
        # srcset contains space-separated "url widthW" pairs — pick largest width
        srcset = el.get_attribute("srcset") or ""
        if srcset:
            best_url, best_w = "", 0
            for entry in srcset.split(","):
                parts = entry.strip().split()
                if len(parts) >= 2:
                    try:
                        w = int(parts[1].rstrip("w"))
                        if w > best_w:
                            best_w, best_url = w, parts[0]
                    except ValueError:
                        pass
                elif len(parts) == 1:
                    best_url = parts[0]
            if best_url:
                return upgrade_shopify_cdn(clean_image_url(best_url))

        # Fall through to explicit attributes
        src = (
            el.get_attribute("data-zoom-src")
            or el.get_attribute("data-large-src")
            or el.get_attribute("data-src")
            or el.get_attribute("src")
            or ""
        )
        return upgrade_shopify_cdn(clean_image_url(src))

    try:
        img_els = page.query_selector_all(sels["extra_imgs"])
        for el in img_els:
            src = best_src_from_element(el)
            if not src:
                continue
            skip_keywords = [
                "placeholder", "blank", "loading", "svg+xml",
                "spinner", "transparent", "data:image", "icon",
            ]
            if any(kw in src.lower() for kw in skip_keywords):
                continue
            width_attr = el.get_attribute("width")
            if width_attr and str(width_attr).isdigit() and int(width_attr) < 80:
                continue
            src = upgrade_shopify_cdn(clean_image_url(src))
            src = resolve_url(src, page_url)
            if src and src not in all_images:
                all_images.append(src)
    except Exception:
        pass

    # Fallback: try the main image selector
    if not all_images:
        try:
            main_el = page.query_selector(sels["main_img"])
            if main_el:
                src = best_src_from_element(main_el)
                if src:
                    src = upgrade_shopify_cdn(clean_image_url(src))
                    src = resolve_url(src, page_url)
                    if src:
                        all_images.append(src)
        except Exception:
            pass

    main_image   = all_images[0] if all_images else ""
    extra_images = list(dict.fromkeys(all_images[1:]))  # deduplicate
    return main_image, extra_images

# ══════════════════════════════════════════════════════════════════════════════
# PRODUCT PAGE SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape_product_page(
    context,            # PATCH 3: persistent context, not raw browser
    url: str,
    sels: dict,
    max_retries: int = 2,
    delay: float = 1.0,
) -> dict:
    """
    UPGRADED: now has per-page retry logic, uses innerHTML (not innerText)
    for the description so HTML formatting is preserved, and waits for
    networkidle so JS-rendered content is fully loaded before extraction.

    Returns empty dict on total failure — caller uses card-level data as fallback.
    """
    last_error = ""

    for attempt in range(1, max_retries + 2):   # 1 … max_retries+1 total tries
        product_page = None
        try:
            product_page = new_stealth_page(context)  # PATCH 3: reuses shared context
            # networkidle = wait until no network requests for 500ms
            # This catches JS-rendered prices and descriptions
            product_page.goto(url, wait_until="networkidle", timeout=30_000)
            # PATCH 2: random jitter — avoids fixed-interval bot fingerprint
            # e.g. delay=1.0 → actual sleep between 0.7s and 1.4s randomly
            time.sleep(delay * random.uniform(0.7, 1.4))

            # ── Title ──────────────────────────────────────────────────────────
            title_el = product_page.query_selector(sels["title"])
            title    = title_el.inner_text().strip() if title_el else ""

            # ── Price ──────────────────────────────────────────────────────────
            price_el = product_page.query_selector(sels["price"])
            price    = clean_price(price_el.inner_text()) if price_el else "0.00"

            # ── Description — use innerHTML to KEEP existing HTML formatting ──
            # innerText strips all tags; innerHTML preserves <p>, <ul> etc.
            # This means Gemini gets richer input and produces better output.
            raw_desc = ""
            desc_el  = product_page.query_selector(sels["desc"])
            if desc_el:
                try:
                    # Try innerHTML first (preserves formatting)
                    inner_html = product_page.evaluate(
                        "(el) => el.innerHTML", desc_el
                    ).strip()
                    # Strip tags to get clean text for Gemini — but keep structure
                    raw_desc = re.sub(r"<[^>]+>", " ", inner_html)
                    raw_desc = re.sub(r"\s+", " ", raw_desc).strip() or inner_html
                except Exception:
                    raw_desc = desc_el.inner_text().strip()

            # ── Images + Variants ──────────────────────────────────────────────
            main_image, extra_images = extract_images(product_page, sels, base_url=product_page.url)
            variants                 = extract_variants(product_page, price, sels)

            return {
                "title":        title,
                "price":        price,
                "raw_desc":     raw_desc,
                "main_image":   main_image,
                "extra_images": extra_images,
                "variants":     variants,
            }

        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            if attempt <= max_retries:
                wait = 2 ** attempt
                print(f"    [page retry {attempt}/{max_retries}] {type(e).__name__} — waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"    [product page] All retries failed ({last_error}) — using card data.")

        finally:
            try:
                if product_page:
                    product_page.close()
            except Exception:
                pass

    return {}  # caller will fall back to card-level data

# ══════════════════════════════════════════════════════════════════════════════
# ROW BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_product_rows(
    handle, title, body_html, price,
    main_image, extra_images, variants,
    source_url: str = "",        # FEATURE: original supplier URL
) -> list[dict]:
    """
    PATCH 1 — maps up to 3 distinct option axes into their own Shopify columns.

    Shopify CSV rules:
    - Row 1:  Title + Body + all variant data + main image + source URL
    - Row 2+: Handle only + variant data + extra image (no title/body repeat)
    - Source URL is only written on row 1 (it's a product-level field)
    """
    rows     = []
    is_first = True

    def empty_row() -> dict:
        return {col: "" for col in SHOPIFY_COLUMNS} | {"Handle": handle}

    for i, variant in enumerate(variants):
        row = empty_row()

        # PATCH 1: map each axis into its own pair of columns
        row["Option1 Name"]  = variant.get("opt1_name",  "Title")
        row["Option1 Value"] = variant.get("opt1_value", "Default Title")
        row["Option2 Name"]  = variant.get("opt2_name",  "")
        row["Option2 Value"] = variant.get("opt2_value", "")
        row["Option3 Name"]  = variant.get("opt3_name",  "")
        row["Option3 Value"] = variant.get("opt3_value", "")
        row["Variant SKU"]   = variant.get("sku",        "")
        row["Variant Price"] = variant.get("price",      price)

        if is_first:
            row["Title"]       = title
            row["Body (HTML)"] = body_html
            row["Image Src"]   = main_image
            row["Source URL"]  = source_url   # FEATURE: raw URL, wrapped in flush_batch
            is_first = False
        else:
            img_index = i - 1
            if img_index < len(extra_images):
                row["Image Src"] = extra_images[img_index]

        rows.append(row)

    # Write any remaining extra images beyond the variant row count
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

    # ── Shopify import CSV — exact columns, no formulas ───────────────────────
    # Drop "Source URL" from the Shopify file — it's not a Shopify field,
    # it's only for the client review. We keep it in SHOPIFY_COLUMNS so
    # build_product_rows can populate it, then strip it before writing.
    shopify_export_cols = [c for c in SHOPIFY_COLUMNS if c != "Source URL"]
    df_shopify   = pd.DataFrame(batch, columns=SHOPIFY_COLUMNS)
    write_header = first_flush and not Path(output_path).exists()
    df_shopify[shopify_export_cols].to_csv(
        output_path, mode="a", index=False,
        header=write_header, encoding="utf-8-sig",
    )

    # ── Client review CSV — readable names + two clickable HYPERLINK columns ──
    df_client         = df_shopify.copy()
    df_client.columns = CLIENT_COLUMNS

    # Clickable image preview
    df_client["Image URL (Click to Preview)"] = df_client[
        "Image URL (Click to Preview)"
    ].apply(
        lambda url: f'=HYPERLINK("{url}","View Image")'
        if isinstance(url, str) and url.startswith("http") else url
    )

    # FEATURE: clickable link to original supplier product page
    df_client["Original Source Link"] = df_client["Original Source Link"].apply(
        lambda url: f'=HYPERLINK("{url}","View Original")'
        if isinstance(url, str) and url.startswith("http") else url
    )

    write_header_client = first_flush and not Path(review_path).exists()
    df_client.to_csv(
        review_path, mode="a", index=False,
        header=write_header_client, encoding="utf-8-sig",
    )

    unique_products = df_shopify["Handle"].nunique()
    print(f"\n  [saved] {unique_products} products ({len(batch)} rows)")
    print(f"  [file1] {output_path}")
    print(f"  [file2] {review_path}")

# ══════════════════════════════════════════════════════════════════════════════
# CARD EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

def extract_cards(catalog_page, context, base_url, sels, skip_handles,
                  batch, first_flush, total_written, batch_size,
                  output_path, review_path, limit, page_num, use_product_page,
                  stats: RunStats, delay: float = 1.0, max_retries: int = 2):

    for _ in range(4):
        catalog_page.evaluate("window.scrollBy(0, window.innerHeight)")
        time.sleep(0.5)

    cards = catalog_page.query_selector_all(sels["card"])
    print(f"  [page {page_num}] {len(cards)} cards found")

    if len(cards) == 0:
        print(f"  [!] No cards found. Try: python \"real wold.py\" --card-sel \".your-class\"")

    for card in cards:
        if total_written + len(batch) >= limit:
            print(f"  [limit] Reached {limit} -- stopping.")
            return batch, first_flush, total_written, True

        try:
            title_el   = card.query_selector(sels["title"])
            card_title = title_el.inner_text().strip() if title_el else ""

            price_el   = card.query_selector(sels["price"])
            card_price = clean_price(price_el.inner_text()) if price_el else "0.00"

            img_el     = card.query_selector("img")
            card_image = ""
            if img_el:
                img_src = img_el.get_attribute("src") or img_el.get_attribute("data-src") or ""
                card_image = resolve_url(clean_image_url(img_src), base_url)

            desc_el   = card.query_selector(sels["desc"])
            card_desc = desc_el.inner_text().strip() if desc_el else ""

            link_el  = card.query_selector(sels["link"])
            href_candidates = []
            if link_el:
                href_candidates.extend([
                    link_el.get_attribute("href"),
                    link_el.get_attribute("data-href"),
                    link_el.get_attribute("data-url"),
                ])
            href_candidates.append(card.get_attribute("data-url"))
            href_candidates.append(card.get_attribute("data-href"))
            href = next((h for h in href_candidates if h), "")
            full_url = resolve_url(href, base_url)

        except Exception as e:
            print(f"    [!] Card error ({type(e).__name__}) -- skipping.")
            continue

        temp_handle = slugify(card_title or "untitled")
        if temp_handle in skip_handles:
            print(f"    [skip] {card_title}")
            continue

        running_total = total_written + len(batch) + 1
        print(f"\n  -- Product {running_total:04d} ------------------------------------------")
        # UPGRADE: show progress bar from RunStats
        print(stats.progress_line())

        # ── UPGRADE: pass delay + max_retries into product page scraper ────────
        page_data    = {}
        failed_url   = ""
        if use_product_page and full_url:
            print(f"  [url]  {full_url}")
            page_data = scrape_product_page(
                context, full_url, sels,   # PATCH 3: context not browser
                max_retries=max_retries,
                delay=delay,
            )
            if not page_data:
                failed_url = full_url   # track for RunStats
        elif use_product_page:
            print(f"  [url]  missing link on card — falling back to card data")
        else:
            print(f"  [mode] Card data only")

        final_title  = page_data.get("title")      or card_title  or "Untitled Product"
        final_price  = page_data.get("price")      or card_price  or "0.00"
        final_desc   = page_data.get("raw_desc")   or card_desc   or ""
        main_image   = page_data.get("main_image") or card_image  or ""
        extra_images = page_data.get("extra_images", [])
        variants     = page_data.get("variants") or [{
            "name": "Title", "value": "Default Title",
            "sku": "", "price": final_price,
        }]
        final_handle = slugify(final_title)

        print(f"  [name]     {final_title}")
        print(f"  [price]    {final_price}")
        print(f"  [images]   {1 + len(extra_images)} total")
        print(f"  [variants] {len(variants)}")

        body_html, used_ai = gemini_format(final_desc)
        print(f"  [AI]       {'Formatted OK' if used_ai else 'Raw fallback used'}")

        product_rows = build_product_rows(
            final_handle, final_title, body_html,
            final_price, main_image, extra_images, variants,
            source_url=full_url,   # FEATURE: pass supplier URL into row builder
        )

        batch.extend(product_rows)
        skip_handles.add(final_handle)
        print(f"  [rows]     {len(product_rows)} rows added to batch")

        # UPGRADE: record into RunStats
        stats.record_product(
            rows=len(product_rows),
            used_ai=used_ai,
            failed_url=failed_url,
        )

        if len(batch) >= batch_size:
            flush_batch(batch, output_path, review_path, first_flush)
            total_written += len(batch)
            first_flush    = False
            batch          = []

    return batch, first_flush, total_written, False

# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION — MODE 1: NEXT BUTTON
# ══════════════════════════════════════════════════════════════════════════════

def scrape_next_button(context, base_url, next_sel, sels, skip_handles,
                       batch_size, output_path, review_path, limit, use_product_page,
                       stats: RunStats, delay: float = 1.0, max_retries: int = 2):
    total_written = 0
    batch         = []
    first_flush   = not Path(output_path).exists()
    page_num      = 0
    catalog_page  = new_stealth_page(context)  # PATCH 3

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
            print(f"  [!] No cards on page {page_num} -- stopping.")
            break

        batch, first_flush, total_written, hit_limit = extract_cards(
            catalog_page, context, base_url, sels, skip_handles,  # PATCH 3
            batch, first_flush, total_written, batch_size,
            output_path, review_path, limit, page_num, use_product_page,
            stats=stats, delay=delay, max_retries=max_retries,
        )
        if hit_limit:
            break

        next_btn = catalog_page.query_selector(next_sel)
        if not next_btn:
            print(f"\n[OK] Last page reached -- no next button found.")
            break

        is_disabled = (
            next_btn.get_attribute("aria-disabled") == "true"
            or "disabled" in (next_btn.get_attribute("class") or "")
        )
        if is_disabled:
            print("\n[OK] Next button disabled -- last page reached.")
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
    return total_written

# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION — MODE 2: URL PATTERN
# ══════════════════════════════════════════════════════════════════════════════

def scrape_url_pattern(context, url_pattern, page_start, page_end, sels, skip_handles,
                       batch_size, output_path, review_path, limit, use_product_page,
                       stats: RunStats, delay: float = 1.0, max_retries: int = 2):
    total_written = 0
    batch         = []
    first_flush   = not Path(output_path).exists()
    catalog_page  = new_stealth_page(context)  # PATCH 3

    for page_num in range(page_start, page_end + 1):
        url = url_pattern.format(page_num)
        print(f"\n{'=' * 60}")
        print(f"  PAGE {page_num} -> {url}")
        print(f"{'=' * 60}")

        try:
            catalog_page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            catalog_page.wait_for_selector(sels["card"], timeout=15_000)
        except PWTimeout:
            print(f"  [OK] No products on page {page_num} -- catalog end reached.")
            break
        except Exception as e:
            print(f"  [!] Page {page_num} failed ({type(e).__name__}) -- skipping.")
            time.sleep(2)
            continue

        count_before = total_written + len(batch)

        batch, first_flush, total_written, hit_limit = extract_cards(
            catalog_page, context, url, sels, skip_handles,  # PATCH 3
            batch, first_flush, total_written, batch_size,
            output_path, review_path, limit, page_num, use_product_page,
            stats=stats, delay=delay, max_retries=max_retries,
        )

        if (total_written + len(batch)) == count_before:
            print(f"  [OK] Page {page_num} has 0 new products -- stopping.")
            break

        if hit_limit:
            break

        time.sleep(1.2)

    if batch:
        flush_batch(batch, output_path, review_path, first_flush)
        total_written += len(batch)

    catalog_page.close()
    return total_written

# ══════════════════════════════════════════════════════════════════════════════
# PAGINATION — MODE 3: SINGLE PAGE
# ══════════════════════════════════════════════════════════════════════════════

def scrape_single_page(context, url, sels, skip_handles,
                       batch_size, output_path, review_path, limit, use_product_page,
                       stats: RunStats, delay: float = 1.0, max_retries: int = 2):
    total_written = 0
    batch         = []
    first_flush   = not Path(output_path).exists()
    catalog_page  = new_stealth_page(context)  # PATCH 3

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
        catalog_page, context, url, sels, skip_handles,  # PATCH 3
        batch, first_flush, total_written, batch_size,
        output_path, review_path, limit, 1, use_product_page,
        stats=stats, delay=delay, max_retries=max_retries,
    )

    if batch:
        flush_batch(batch, output_path, review_path, first_flush)
        total_written += len(batch)

    catalog_page.close()
    return total_written

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args        = parse_args()
    sels        = build_selectors(args)
    review_path = derive_review_path(args.output)
    use_pp      = not args.no_product_page

    # UPGRADE: start file logging immediately so every line is captured
    setup_logging(args.log_file)

    # UPGRADE: initialise run stats tracker
    stats = RunStats(limit=args.limit)

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
    print(f"  Request delay         : {args.delay}s")        # NEW
    print(f"  Max page retries      : {args.max_retries}")   # NEW
    print(f"  Browser visible       : {'YES' if args.show_browser else 'NO'}")  # NEW
    print(f"  Shopify CSV           : {args.output}")
    print(f"  Client Review CSV     : {review_path}")
    print(f"  Log file              : {args.log_file}")      # NEW
    print(f"{'=' * 60}\n")

    # UPGRADE: pass all new args into the common dict
    common = dict(
        sels             = sels,
        skip_handles     = skip_handles,
        batch_size       = args.batch,
        output_path      = args.output,
        review_path      = review_path,
        limit            = args.limit,
        use_product_page = use_pp,
        stats            = stats,
        delay            = args.delay,
        max_retries      = args.max_retries,
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless = not args.show_browser,
            args     = [
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )

        # PATCH 3: single persistent context — stealth config applied once here,
        # shared across ALL pages (catalog page + every product page tab).
        # This avoids creating a new browser context per product which
        # causes memory leaks and eventual crashes on large catalogs.
        context = browser.new_context(
            user_agent         = STEALTH_UA,
            viewport           = STEALTH_VIEWPORT,
            extra_http_headers = STEALTH_HEADERS,
        )

        if args.url_pattern:
            print(f"[mode] URL PATTERN -- pages {args.page_start} to {args.page_end}")
            written = scrape_url_pattern(
                context, args.url_pattern,   # PATCH 3: context not browser
                args.page_start, args.page_end,
                **common,
            )

        elif args.next_sel:
            base_url = load_target_url()
            print(f"[mode] NEXT BUTTON -- '{args.next_sel}'")
            written = scrape_next_button(context, base_url, args.next_sel, **common)  # PATCH 3

        else:
            base_url = load_target_url()
            print(f"[mode] SINGLE PAGE")
            written = scrape_single_page(context, base_url, **common)  # PATCH 3

        context.close()
        browser.close()

    if written == 0:
        print(f"\n{'=' * 60}")
        print(f"  [X] NOTHING WRITTEN")
        print(f"  Check your selectors or target URL")
        print(f"{'=' * 60}")
        raise SystemExit(1)

    # UPGRADE: rich final summary from RunStats
    print(stats.summary())
    print(f"  Shopify file : {args.output}")
    print(f"  Client file  : {review_path}")
    print(f"{'=' * 60}")

    # UPGRADE: save failed URLs to file for manual review or re-run
    stats.save_failed_urls(output_dir=str(Path(args.output).parent))