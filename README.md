# Universal E-Commerce ETL Pipeline
**Author:** Jituparna Dihingia  
**Domain:** B2B Private Intelligence & Data Automation  
**Live Repository:** [Shopify-product-automation](https://github.com/PAPUDIHINGIA/Shopify-product-automation)

## Overview
A production-grade ETL (Extract, Transform, Load) pipeline built with Python, Playwright, and Pandas. This architecture is designed to navigate complex e-commerce DOM structures, bypass anti-bot mechanisms, and extract multi-variant product data into structured client deliverables. 

This tool does not just scrape; it logically audits visual selectors, intercepts hidden JSON-LD payloads, and utilizes Large Language Models (LLMs) to sanitize and reformat unstructured data into strict HTML schemas.

## Core Architecture
* **Heuristic DOM Auditing:** Dynamically locates product variants through visual selectors, JSON-LD intercepts, and window-level analytics objects (Shopify/WooCommerce).
* **AI Data Transformation:** Integrates the Google GenAI (Gemini 2.5 Flash) API to automatically clean, reformat, and generate strict HTML outputs for unstructured descriptions, featuring exponential backoff and raw-text fallbacks for high availability.
* **State Management:** Utilizes a custom `RunStats` class to track AI hallucination rates, request failures, and extraction yields in real-time.
* **Data Structuring:** Maps multi-dimensional variant arrays and image clusters into flat, client-ready CSV schemas (Shopify Import format & Client Review format) using Pandas.
* **Stealth & Evasion:** Implements Playwright with custom User-Agents, viewport randomization, and automated popup dismissal to ensure uninterrupted extraction.

## Prerequisites
* Python 3.10+
* `playwright` (with Chromium binaries installed)
* `pandas`
* `google-genai`
* `python-dotenv`

## Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/PAPUDIHINGIA/Shopify-product-automation.git](https://github.com/PAPUDIHINGIA/Shopify-product-automation.git)
   cd Shopify-product-automation
