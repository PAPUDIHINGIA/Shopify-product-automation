import os
import re
import time
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import google.generativeai as genai

load_dotenv()
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

# ── CONFIG ─────────────────────────────────────────────────────────────────────
TARGET_URL   = "INSERT_CLIENT_URL_HERE"   # ← swap before running
PRODUCT_LIMIT = 10
OUTPUT_FILE  = "sample_upload.csv"

# ── SELECTORS (tune these per-site) ────────────────────────────────────────────
# These cover the most common e-commerce grid conventions.
# If 0 products are found, open DevTools on the target site and inspect one card.
SEL_PRODUCT_CARD  = (
    "[class*='product-card'], [class*='product-item'], "
    "[class*='ProductCard'], article[data-product], li[data-product-id]"
)
SEL_TITLE         = "h2, h3, [class*='product-title'], [class*='ProductName']"
SEL_PRICE         = "[class*='price']:not([class*='compare']), [data-price]"
SEL_IMAGE         = "img"                          # first <img> inside the card
SEL_DESCRIPTION   = "[class*='description'], [class*='excerpt'], p"  # optional field


# ── HELPERS ────────────────────────────────────────────────────────────────────
def slugify(title: str) -> str:
    """'Blue Widget Pro 500g' → 'blue-widget-pro-500g'"""
    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def clean_price(raw: str) -> str:
    """Strip currency symbols / whitespace → bare decimal string."""
    return re.sub(r"[^\d.]", "", raw).strip() or "0.00"


def gemini_format(raw_text: str) -> str:
    """
    Send raw description text to Gemini and get back clean Shopify-ready HTML.
    Returns a plain <p>/<ul> string — no markdown fences, no extra commentary.
    """
    if not raw_text.strip():
        return "<p>No description available.</p>"

    model = genai.GenerativeModel("gemini-2.5-flash")
    prompt = (
        "You are a Shopify product copywriter. "
        "Rewrite the following raw product text as clean HTML only. "
        "Use <p> tags for descriptive sentences and <ul><li> tags for feature bullets. "
        "Output ONLY the HTML — no markdown code fences, no explanation, no extra text.\n\n"
        f"RAW TEXT:\n{raw_text}"
    )
    response = model.generate_content(prompt)
    html = response.text.strip()

    # Defensive strip — Gemini occasionally wraps output in ```html ... ```
    html = re.sub(r"^```[a-z]*\n?", "", html)
    html = re.sub(r"\n?```$", "", html)
    return html.strip()


# ── SCRAPER ────────────────────────────────────────────────────────────────────
def scrape_products(url: str, limit: int) -> list[dict]:
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )

        print(f"[→] Navigating to {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)

        # Wait for at least one product card to appear; adjust selector if needed
        try:
            page.wait_for_selector(SEL_PRODUCT_CARD, timeout=20_000)
        except Exception:
            print("[!] Product cards not found with default selector. "
                  "Inspect the page and update SEL_PRODUCT_CARD.")
            browser.close()
            return []

        # Optional: scroll to trigger lazy-loaded images
        for _ in range(3):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.8)

        cards = page.query_selector_all(SEL_PRODUCT_CARD)
        print(f"[✓] Found {len(cards)} product cards — extracting first {limit}")

        for card in cards[:limit]:
            # ── Title ──────────────────────────────────────────────────────────
            title_el = card.query_selector(SEL_TITLE)
            title = title_el.inner_text().strip() if title_el else "Untitled Product"

            # ── Price ──────────────────────────────────────────────────────────
            price_el = card.query_selector(SEL_PRICE)
            price = clean_price(price_el.inner_text()) if price_el else "0.00"

            # ── Image ──────────────────────────────────────────────────────────
            img_el = card.query_selector(SEL_IMAGE)
            image_url = ""
            if img_el:
                # Some sites use data-src / data-lazy-src for lazy loading
                image_url = (
                    img_el.get_attribute("src")
                    or img_el.get_attribute("data-src")
                    or img_el.get_attribute("data-lazy-src")
                    or ""
                )
            # Resolve protocol-relative URLs
            if image_url.startswith("//"):
                image_url = "https:" + image_url

            # ── Description (best-effort) ──────────────────────────────────────
            # Many grids don't expose descriptions — the field may be empty.
            # If so, Gemini will return a polite fallback message.
            desc_el = card.query_selector(SEL_DESCRIPTION)
            raw_desc = desc_el.inner_text().strip() if desc_el else ""

            products.append({
                "title":    title,
                "price":    price,
                "image":    image_url,
                "raw_desc": raw_desc,
            })
            print(f"  [{len(products):02d}] {title} — £/$/€{price}")

        browser.close()

    return products


# ── PIPELINE ───────────────────────────────────────────────────────────────────
def build_shopify_csv(products: list[dict]) -> pd.DataFrame:
    rows = []
    for i, p in enumerate(products, start=1):
        print(f"[Gemini {i}/{len(products)}] Formatting: {p['title']}")
        body_html = gemini_format(p["raw_desc"])

        rows.append({
            "Handle":       slugify(p["title"]),
            "Title":        p["title"],
            "Body (HTML)":  body_html,
            "Variant Price": p["price"],
            "Image Src":    p["image"],
        })

    return pd.DataFrame(rows, columns=[
        "Handle", "Title", "Body (HTML)", "Variant Price", "Image Src"
    ])


# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    raw_products = scrape_products(TARGET_URL, PRODUCT_LIMIT)

    if not raw_products:
        print("[✗] No products scraped. Exiting.")
        raise SystemExit(1)

    df = build_shopify_csv(raw_products)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")  # utf-8-sig for Excel compat
    print(f"\n[✓] Exported {len(df)} products → {OUTPUT_FILE}")
    print(df[["Handle", "Title", "Variant Price"]].to_string(index=False))