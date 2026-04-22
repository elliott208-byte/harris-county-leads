#!/usr/bin/env python3
"""
Harris County Motivated Seller Lead Scraper
============================================
Scrapes Harris County Clerk portal for motivated-seller document types,
enriches records with HCAD parcel data, scores leads, and outputs JSON + CSV.

Usage:
    python scraper/fetch.py

Outputs:
    dashboard/records.json
    data/records.json
    data/ghl_export.csv
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Optional dbfread ──────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("hc_scraper")

# ── Constants ─────────────────────────────────────────────────────────────────
CLERK_BASE    = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"
CLERK_DOC     = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"
HCAD_YEAR     = datetime.now().year
LOOKBACK_DAYS  = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 4  # seconds between retries

# HCAD bulk data candidates (tried in order)
HCAD_CANDIDATES = [
    f"https://pdata.hcad.org/data/{HCAD_YEAR}/real_acct.zip",
    f"https://pdata.hcad.org/data/{HCAD_YEAR - 1}/real_acct.zip",
    f"https://pdata.hcad.org/data/{HCAD_YEAR}/comm_acct.zip",
    f"https://pdata.hcad.org/data/{HCAD_YEAR - 1}/comm_acct.zip",
]

# ── Document type catalogue ───────────────────────────────────────────────────
DOC_TYPES = {
    "LP":       ("Lis Pendens",              "lis_pendens"),
    "NOFC":     ("Notice of Foreclosure",    "foreclosure"),
    "TAXDEED":  ("Tax Deed",                 "tax_deed"),
    "JUD":      ("Judgment",                 "judgment"),
    "CCJ":      ("Certified Judgment",       "judgment"),
    "DRJUD":    ("Domestic Judgment",        "judgment"),
    "LNCORPTX": ("Corp Tax Lien",            "tax_lien"),
    "LNIRS":    ("IRS Lien",                 "tax_lien"),
    "LNFED":    ("Federal Lien",             "tax_lien"),
    "LN":       ("Lien",                     "lien"),
    "LNMECH":   ("Mechanic Lien",            "lien"),
    "LNHOA":    ("HOA Lien",                 "lien"),
    "MEDLN":    ("Medicaid Lien",            "lien"),
    "PRO":      ("Probate Document",         "probate"),
    "NOC":      ("Notice of Commencement",   "noc"),
    "RELLP":    ("Release Lis Pendens",      "release"),
}

CAT_LABELS = {
    "lis_pendens": "Lis Pendens",
    "foreclosure": "Pre-Foreclosure",
    "tax_deed":    "Tax Deed",
    "judgment":    "Judgment / Lien",
    "tax_lien":    "Tax Lien",
    "lien":        "Lien",
    "probate":     "Probate / Estate",
    "noc":         "Notice of Commencement",
    "release":     "Release",
}

# ── Output paths ──────────────────────────────────────────────────────────────
ROOT           = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = ROOT / "dashboard" / "records.json"
DATA_JSON      = ROOT / "data"      / "records.json"
GHL_CSV        = ROOT / "data"      / "ghl_export.csv"


# =============================================================================
# SECTION 1 – HCAD Parcel Lookup
# =============================================================================

def _retry_get(url: str, stream: bool = False, **kwargs) -> Optional[requests.Response]:
    """HTTP GET with exponential-backoff retry."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(url, stream=stream, timeout=90, **kwargs)
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning(f"GET {url} attempt {attempt}/{RETRY_ATTEMPTS} failed: {exc}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    return None


def _parse_amount(text: str) -> float:
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").upper().strip())


def _name_variants(name: str) -> list:
    name = _normalize_name(name)
    variants = {name}
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            variants.add(f"{parts[1]} {parts[0]}")
            variants.add(f"{parts[0]} {parts[1]}")
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            variants.add(f"{tokens[-1]}, {' '.join(tokens[:-1])}")
            variants.add(f"{tokens[-1]} {' '.join(tokens[:-1])}")
    return [v for v in variants if v]


class HCADLookup:
    """Downloads HCAD real-property account file and builds owner->address index."""

    def __init__(self):
        self._index: dict = {}
        self._loaded = False

    def load(self):
        if self._loaded:
            return
        log.info("Loading HCAD parcel data ...")
        for url in HCAD_CANDIDATES:
            log.info(f"  Trying {url}")
            r = _retry_get(url, stream=True)
            if r is None:
                continue
            try:
                raw = b""
                for chunk in r.iter_content(chunk_size=1 << 20):
                    raw += chunk
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    names = zf.namelist()
                    log.info(f"  Zip contents: {names}")
                    target = next(
                        (n for n in names if n.lower().endswith((".txt", ".dbf"))), None
                    )
                    if target is None:
                        continue
                    data = zf.read(target)
                    if target.lower().endswith(".dbf"):
                        self._load_dbf_bytes(data)
                    else:
                        self._load_txt_bytes(data)
                if self._index:
                    log.info(f"  Loaded {len(self._index):,} parcel records.")
                    self._loaded = True
                    return
            except Exception as exc:
                log.warning(f"  Failed to parse {url}: {exc}")

        log.warning("HCAD parcel data unavailable - address enrichment disabled.")
        self._loaded = True

    def _load_txt_bytes(self, data: bytes):
        text = data.decode("latin-1", errors="replace")
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        col_map = self._detect_columns(reader.fieldnames or [])
        for row in reader:
            try:
                self._index_row(row, col_map)
            except Exception:
                pass

    def _load_dbf_bytes(self, data: bytes):
        if not HAS_DBF:
            log.warning("dbfread not installed; skipping DBF parse.")
            return
        tmp = Path("/tmp/hcad_parcel.dbf")
        tmp.write_bytes(data)
        try:
            table = DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True)
            col_map = self._detect_columns(table.field_names)
            for row in table:
                try:
                    self._index_row(dict(row), col_map)
                except Exception:
                    pass
        finally:
            tmp.unlink(missing_ok=True)

    @staticmethod
    def _detect_columns(fields: list) -> dict:
        upper = {f.upper(): f for f in fields}
        def pick(*candidates):
            for c in candidates:
                if c.upper() in upper:
                    return upper[c.upper()]
            return None
        return {
            "owner":      pick("OWN1", "OWNER", "OWNER_NAME", "NAME"),
            "site_addr":  pick("SITEADDR", "SITE_ADDR", "SITE_ADDRESS", "PROP_ADDR"),
            "site_city":  pick("SITE_CITY", "SITECITY", "PROP_CITY"),
            "site_zip":   pick("SITE_ZIP",  "SITEZIP",  "PROP_ZIP"),
            "mail_addr":  pick("MAILADR1", "ADDR_1", "MAIL_ADDR", "MAIL_ADDRESS"),
            "mail_city":  pick("MAILCITY", "CITY",   "MAIL_CITY"),
            "mail_state": pick("MAILSTATE", "STATE",  "MAIL_STATE"),
            "mail_zip":   pick("MAILZIP",  "ZIP",    "MAIL_ZIP"),
        }

    def _index_row(self, row: dict, col_map: dict):
        def g(key):
            col = col_map.get(key)
            return str(row.get(col, "") or "").strip() if col else ""
        owner = g("owner")
        if not owner:
            return
        parcel = {
            "prop_address": g("site_addr"),
            "prop_city":    g("site_city") or "Houston",
            "prop_state":   "TX",
            "prop_zip":     g("site_zip"),
            "mail_address": g("mail_addr"),
            "mail_city":    g("mail_city"),
            "mail_state":   g("mail_state") or "TX",
            "mail_zip":     g("mail_zip"),
        }
        for variant in _name_variants(owner):
            if variant and variant not in self._index:
                self._index[variant] = parcel

    def lookup(self, name: str) -> Optional[dict]:
        for variant in _name_variants(name):
            result = self._index.get(variant)
            if result:
                return result
        return None


# =============================================================================
# SECTION 2 – Harris County Clerk Scraper (Playwright async)
# =============================================================================

async def _fill_and_search(page, doc_code: str, date_from: str, date_to: str) -> list:
    records = []
    try:
        await page.goto(CLERK_DOC, wait_until="networkidle", timeout=45_000)

        date_from_sel = "input#txtDateFrom, input[id*='DateFrom'], input[name*='DateFrom']"
        date_to_sel   = "input#txtDateTo, input[id*='DateTo'], input[name*='DateTo']"
        await page.fill(date_from_sel, date_from)
        await page.fill(date_to_sel,   date_to)

        instr_sel = (
            "select#ddlInstrumentType, select[id*='InstrumentType'], "
            "select[name*='InstrumentType']"
        )
        try:
            await page.select_option(instr_sel, value=doc_code)
        except Exception:
            try:
                await page.select_option(instr_sel, label=doc_code)
            except Exception:
                await page.fill(
                    "input[id*='InstrumentType'], input[name*='InstrumentType']",
                    doc_code,
                )

        submit_sel = (
            "input[type='submit']#btnSearch, input[type='submit'][value*='Search'], "
            "button#btnSearch, button[id*='Search']"
        )
        await page.click(submit_sel, timeout=10_000)
        await page.wait_for_load_state("networkidle", timeout=45_000)

        page_num = 0
        while True:
            page_num += 1
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            rows = _parse_result_table(soup, doc_code)
            records.extend(rows)
            log.info(f"    [{doc_code}] page {page_num}: {len(rows)} rows")

            next_link = soup.find("a", string=re.compile(r"^\s*(Next|>)\s*$", re.I))
            if not next_link or page_num > 50:
                break
            try:
                await page.click("a:text-matches('Next|>', 'i')", timeout=8_000)
                await page.wait_for_load_state("networkidle", timeout=30_000)
            except PWTimeout:
                break

    except Exception as exc:
        log.warning(f"  [{doc_code}] scrape error: {exc}")

    return records


def _parse_result_table(soup: BeautifulSoup, doc_code: str) -> list:
    records = []
    table = soup.find("table", id=re.compile(r"Grid|Result|gvResult|grdResult", re.I))
    if table is None:
        for t in soup.find_all("table"):
            if len(t.find_all("th")) >= 3:
                table = t
                break
    if table is None:
        return records

    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]

    def col(cells, *names):
        for name in names:
            for i, h in enumerate(headers):
                if name in h and i < len(cells):
                    return cells[i].get_text(strip=True)
        return ""

    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        try:
            link_tag  = tr.find("a", href=True)
            clerk_url = ""
            if link_tag:
                href = link_tag["href"]
                clerk_url = href if href.startswith("http") else urljoin(CLERK_BASE, href)

            doc_num = col(cells, "file", "instrument", "doc", "number")
            if not doc_num and link_tag:
                doc_num = link_tag.get_text(strip=True)
            if not doc_num:
                continue

            records.append({
                "doc_num":   doc_num,
                "doc_type":  doc_code,
                "filed":     col(cells, "date", "filed", "record"),
                "owner":     col(cells, "grantor", "owner", "from"),
                "grantee":   col(cells, "grantee", "to", "lender"),
                "legal":     col(cells, "legal", "description", "subdivision"),
                "amount":    col(cells, "amount", "consideration", "debt"),
                "clerk_url": clerk_url,
            })
        except Exception as exc:
            log.debug(f"Row parse error: {exc}")

    return records


async def scrape_clerk(date_from: str, date_to: str) -> list:
    all_records = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        try:
            await page.goto(CLERK_DOC, wait_until="networkidle", timeout=30_000)
            await page.click(
                "button:has-text('Accept'), button:has-text('OK'), "
                "button:has-text('Close'), button:has-text('Agree')",
                timeout=4_000,
            )
        except Exception:
            pass

        for doc_code in DOC_TYPES:
            log.info(f"  Scraping doc type: {doc_code}")
            for attempt in range(1, RETRY_ATTEMPTS + 1):
                try:
                    rows = await _fill_and_search(page, doc_code, date_from, date_to)
                    all_records.extend(rows)
                    break
                except Exception as exc:
                    log.warning(f"  [{doc_code}] attempt {attempt} failed: {exc}")
                    if attempt < RETRY_ATTEMPTS:
                        await asyncio.sleep(RETRY_DELAY * attempt)

        await browser.close()

    log.info(f"Clerk scrape complete: {len(all_records)} raw records")
    return all_records


# =============================================================================
# SECTION 3 – Scoring Engine
# =============================================================================

def compute_flags(record: dict, today: datetime) -> list:
    flags    = []
    cat      = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    owner    = (record.get("owner") or "").upper()

    if cat == "lis_pendens":              flags.append("Lis pendens")
    if cat == "foreclosure":              flags.append("Pre-foreclosure")
    if cat == "judgment":                 flags.append("Judgment lien")
    if cat in ("tax_lien", "tax_deed"):   flags.append("Tax lien")
    if doc_type == "LNMECH":              flags.append("Mechanic lien")
    if cat == "probate":                  flags.append("Probate / estate")
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|L\.L\.C", owner):
        flags.append("LLC / corp owner")

    try:
        filed_dt = datetime.strptime(record.get("filed", ""), "%m/%d/%Y")
        if (today - filed_dt).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    return flags


def compute_score(record: dict, flags: list) -> int:
    score = 30
    score += 10 * len(flags)

    owner_cats = record.get("_owner_cats", [])
    if "lis_pendens" in owner_cats and "foreclosure" in owner_cats:
        score += 20

    amount = record.get("_amount_num", 0.0)
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10

    if "New this week" in flags: score += 5
    if record.get("prop_address"): score += 5

    return min(score, 100)


# =============================================================================
# SECTION 4 – Enrichment & Assembly
# =============================================================================

def enrich_records(raw: list, hcad: HCADLookup, today: datetime) -> list:
    enriched = []
    for r in raw:
        try:
            doc_type  = r.get("doc_type", "")
            cat_info  = DOC_TYPES.get(doc_type, ("Unknown", "unknown"))
            cat_label, cat = cat_info[0], cat_info[1]
            amount_str = r.get("amount", "")
            amount_num = _parse_amount(amount_str)
            owner      = (r.get("owner") or "").strip()
            parcel     = hcad.lookup(owner) if owner else None

            rec = {
                "doc_num":      r.get("doc_num", ""),
                "doc_type":     doc_type,
                "filed":        r.get("filed", ""),
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner,
                "grantee":      (r.get("grantee") or "").strip(),
                "amount":       amount_str,
                "legal":        (r.get("legal") or "").strip(),
                "prop_address": parcel["prop_address"] if parcel else "",
                "prop_city":    parcel["prop_city"]    if parcel else "",
                "prop_state":   parcel["prop_state"]   if parcel else "TX",
                "prop_zip":     parcel["prop_zip"]     if parcel else "",
                "mail_address": parcel["mail_address"] if parcel else "",
                "mail_city":    parcel["mail_city"]    if parcel else "",
                "mail_state":   parcel["mail_state"]   if parcel else "TX",
                "mail_zip":     parcel["mail_zip"]     if parcel else "",
                "clerk_url":    r.get("clerk_url", ""),
                "_amount_num":  amount_num,
                "_owner_cats":  [],
            }
            enriched.append(rec)
        except Exception as exc:
            log.debug(f"Enrich error: {exc}")

    owner_cats: dict = {}
    for rec in enriched:
        owner_cats.setdefault(rec["owner"], []).append(rec["cat"])
    for rec in enriched:
        rec["_owner_cats"] = owner_cats.get(rec["owner"], [])

    for rec in enriched:
        flags = compute_flags(rec, today)
        score = compute_score(rec, flags)
        rec["flags"] = flags
        rec["score"] = score
        rec.pop("_amount_num", None)
        rec.pop("_owner_cats", None)

    enriched.sort(key=lambda x: x["score"], reverse=True)
    return enriched


# =============================================================================
# SECTION 5 – GHL CSV Export
# =============================================================================

GHL_COLUMNS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def _split_name(full_name: str):
    name = full_name.strip()
    if "," in name:
        parts = [p.strip().title() for p in name.split(",", 1)]
        return parts[1], parts[0]
    tokens = name.split()
    if len(tokens) >= 2:
        return " ".join(tokens[:-1]).title(), tokens[-1].title()
    return name.title(), ""


def export_ghl_csv(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_COLUMNS)
        writer.writeheader()
        for rec in records:
            first, last = _split_name(rec.get("owner", ""))
            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        rec.get("mail_address", ""),
                "Mailing City":           rec.get("mail_city", ""),
                "Mailing State":          rec.get("mail_state", "TX"),
                "Mailing Zip":            rec.get("mail_zip", ""),
                "Property Address":       rec.get("prop_address", ""),
                "Property City":          rec.get("prop_city", ""),
                "Property State":         rec.get("prop_state", "TX"),
                "Property Zip":           rec.get("prop_zip", ""),
                "Lead Type":              rec.get("cat_label", ""),
                "Document Type":          rec.get("doc_type", ""),
                "Date Filed":             rec.get("filed", ""),
                "Document Number":        rec.get("doc_num", ""),
                "Amount/Debt Owed":       rec.get("amount", ""),
                "Seller Score":           rec.get("score", 0),
                "Motivated Seller Flags": " | ".join(rec.get("flags", [])),
                "Source":                 "Harris County Clerk",
                "Public Records URL":     rec.get("clerk_url", ""),
            })
    log.info(f"GHL CSV saved: {path} ({len(records)} rows)")


# =============================================================================
# SECTION 6 – JSON Output
# =============================================================================

def save_json(records: list, date_from: str, date_to: str):
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Harris County Clerk - cclerk.hctx.net",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    for path in (DASHBOARD_JSON, DATA_JSON):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        log.info(f"JSON saved: {path}")


# =============================================================================
# SECTION 7 – Main
# =============================================================================

async def main():
    today     = datetime.utcnow()
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")

    log.info("=" * 60)
    log.info(f"Harris County Lead Scraper  |  {date_from} -> {date_to}")
    log.info("=" * 60)

    hcad = HCADLookup()
    hcad.load()

    log.info("Scraping Harris County Clerk portal ...")
    raw_records = await scrape_clerk(date_from, date_to)

    log.info("Enriching and scoring records ...")
    records = enrich_records(raw_records, hcad, today)

    save_json(records, date_from, date_to)
    export_ghl_csv(records, GHL_CSV)

    log.info(
        f"Done. {len(records)} leads | "
        f"{sum(1 for r in records if r.get('prop_address'))} with address"
    )


if __name__ == "__main__":
    asyncio.run(main())
