#!/usr/bin/env python3
"""
HEPData Downloader & Analyzer
===============================
Downloads published measurement tables from HEPData (hepdata.net) and
provides analysis tools for comparing with theoretical predictions.

Designed to pair with Paper V (Multi-Kink Kaluza-Klein Flavor Physics)
for direct comparison of:
  - B0→K*0μ+μ- angular observables (FL, AFB, P'5) vs C9/C'9 predictions
  - Bs→μ+μ- branching ratio vs MKK bounds
  - High-mass dilepton spectra vs Z' exclusion limits
  - ΔF=2 Wilson coefficient constraints vs inverted FCNC hierarchy

Usage:
    python hepdata_downloader.py --help
    python hepdata_downloader.py search --query "B0 K* mu mu angular" --collaboration CMS
    python hepdata_downloader.py fetch --inspire 2850101 --format csv --output ./hepdata
    python hepdata_downloader.py fetch-all --output ./hepdata   (downloads all Paper V-relevant)
    python hepdata_downloader.py list-tables --inspire 2850101
    python hepdata_downloader.py catalog ./hepdata

Requirements:
    pip install requests pyyaml
    (Optional: pip install pandas matplotlib for analysis)
"""

import argparse
import json
import os
import sys
import time
import logging
import tarfile
import zipfile
import io
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

try:
    import yaml
except ImportError:
    yaml = None

try:
    import pandas as pd
except ImportError:
    pd = None

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("hepdata_downloader")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HEPDATA_API = "https://www.hepdata.net"
SEARCH_URL = f"{HEPDATA_API}/search/"
RECORD_URL = f"{HEPDATA_API}/record/ins{{inspire_id}}"
DOWNLOAD_URL = f"{HEPDATA_API}/record/ins{{inspire_id}}?format={{fmt}}"
TABLE_URL = f"{HEPDATA_API}/record/ins{{inspire_id}}?format={{fmt}}&table={{table}}"

DEFAULT_OUTPUT = "./hepdata"

# ---------------------------------------------------------------------------
# Paper V-relevant records: curated list
# ---------------------------------------------------------------------------

PAPER_V_RECORDS = {
    # ─── B→K*μμ angular observables (your C'9/C9 ~ 0.2 prediction) ───
    "cms_bkstarmumu_8tev": {
        "inspire_id": 1385600,
        "title": "CMS B0→K*0μμ angular analysis √s=8 TeV",
        "journal": "Phys.Lett.B 753 (2016) 424-448",
        "relevance": "FL, AFB, dB/dq² in q² bins. Direct test of C9/C'9 chiral structure.",
        "tables_of_interest": ["Table 1", "Table 2"],
    },
    "cms_bkstarmumu_13tev": {
        "inspire_id": 2850101,
        "title": "CMS B0→K*0μμ angular analysis √s=13 TeV",
        "journal": "Phys.Lett.B (2025) 139406",
        "relevance": "P1-P3, P'4-P'8, FL in q² bins. 15 tables. Best CMS P'5 measurement.",
        "tables_of_interest": ["Results P5p", "Results Fl", "Results P1"],
    },
    "cms_bpkstarmumu_8tev": {
        "inspire_id": 1826544,
        "title": "CMS B+→K*+μμ angular analysis √s=8 TeV",
        "journal": "JHEP 04 (2021) 124",
        "relevance": "Independent B→K*μμ channel. FL, AFB cross-check.",
        "tables_of_interest": ["Table 1"],
    },

    # ─── Bs→μμ branching ratio (your MKK ≳ 10-20 TeV bound) ───
    "cms_lhcb_bsmumu": {
        "inspire_id": 1328493,
        "title": "CMS+LHCb Bs→μμ observation (combined)",
        "journal": "Nature 522 (2015) 68-72",
        "relevance": "First observation of Bs→μμ. BR measurement constrains |C10-C'10|.",
        "tables_of_interest": [],  # may not have HEPData tables
    },

    # ─── High-mass dilepton (Z' exclusion floor) ───
    "cms_highmass_dilepton_bjets": {
        "inspire_id": 2935112,
        "title": "CMS high-mass dilepton + b-jets √s=13 TeV",
        "journal": "CMS-EXO-23-010 (2025)",
        "relevance": "bsll and bbll contact interaction limits. Direct NP scale bounds.",
        "tables_of_interest": [],
    },
    "atlas_highmass_dilepton": {
        "inspire_id": 1802523,
        "title": "ATLAS high-mass dilepton non-resonant √s=13 TeV",
        "journal": "JHEP 11 (2020) 005",
        "relevance": "Contact interaction limits, Z' exclusion. 139 fb⁻¹ dataset.",
        "tables_of_interest": [],
    },

    # ─── UTfit / flavor constraints (your inverted hierarchy prediction) ───
    # UTfit results aren't on HEPData but we note them here for completeness.
}


# ---------------------------------------------------------------------------
# API functions
# ---------------------------------------------------------------------------

def search_hepdata(query: str, collaboration: str = None, page: int = 1,
                   size: int = 10) -> dict:
    """Search HEPData for records matching query."""
    params = {"q": query, "page": page, "size": size}
    if collaboration:
        params["q"] += f' AND collaborations:"{collaboration}"'

    resp = requests.get(SEARCH_URL, params=params, timeout=30,
                        headers={"Accept": "application/json"})
    resp.raise_for_status()
    return resp.json()


def get_record_info(inspire_id: int) -> dict:
    """Get JSON metadata for a HEPData record by INSPIRE ID."""
    url = f"{HEPDATA_API}/record/ins{inspire_id}?format=json&light=true"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def list_tables(inspire_id: int) -> list:
    """List table names in a HEPData record."""
    data = get_record_info(inspire_id)
    tables = []
    if "record" in data:
        rec = data["record"]
        # HEPData JSON has table info under various keys
        if "data_tables" in rec:
            for t in rec["data_tables"]:
                tables.append(t.get("name", ""))
    # Fallback: try fetching the full JSON
    if not tables:
        url = f"{HEPDATA_API}/record/ins{inspire_id}?format=json"
        resp = requests.get(url, timeout=60)
        if resp.ok:
            full = resp.json()
            if isinstance(full, dict):
                for key in full:
                    if "table" in key.lower() or "result" in key.lower():
                        tables.append(key)
                # Try nested structure
                if "tables" in full:
                    tables = [t.get("name", str(i)) for i, t in enumerate(full["tables"])]
    return tables


def download_record(inspire_id: int, fmt: str = "csv", output_dir: str = DEFAULT_OUTPUT,
                    table: str = None) -> Path:
    """
    Download a HEPData record (or specific table) in the given format.

    fmt: csv, json, yaml, root, yoda
    Returns path to downloaded file/directory.
    """
    record_dir = Path(output_dir) / f"ins{inspire_id}"
    record_dir.mkdir(parents=True, exist_ok=True)

    if table:
        # Download single table
        url = f"{HEPDATA_API}/record/ins{inspire_id}?format={fmt}&table={table}"
        log.info(f"Downloading table '{table}' from ins{inspire_id} as {fmt}")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()

        safe_name = table.replace(" ", "_").replace("/", "_")
        ext = fmt if fmt != "yaml" else "yaml"
        outpath = record_dir / f"{safe_name}.{ext}"
        outpath.write_bytes(resp.content)
        log.info(f"Saved → {outpath}")
        return outpath
    else:
        # Download entire record (comes as .tar.gz for csv/yaml/root/yoda, .json for json)
        url = f"{HEPDATA_API}/record/ins{inspire_id}?format={fmt}"
        log.info(f"Downloading full record ins{inspire_id} as {fmt}")
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")

        if fmt == "json":
            outpath = record_dir / f"record.json"
            outpath.write_bytes(resp.content)
            log.info(f"Saved → {outpath}")
            return outpath

        # For csv/yaml/root/yoda: comes as tar.gz archive
        archive_path = record_dir / f"record.tar.gz"

        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(archive_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = downloaded / total * 100
                    print(f"\r  Downloading: {pct:.1f}% ({downloaded:,} / {total:,} bytes)", end="")
        print()

        # Extract
        if tarfile.is_tarfile(str(archive_path)):
            log.info(f"Extracting archive...")
            with tarfile.open(archive_path, "r:gz") as tf:
                tf.extractall(path=str(record_dir))
            archive_path.unlink()
            log.info(f"Extracted → {record_dir}")
        elif zipfile.is_zipfile(str(archive_path)):
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(path=str(record_dir))
            archive_path.unlink()
            log.info(f"Extracted → {record_dir}")
        else:
            # Raw file, just rename
            ext = fmt
            final = record_dir / f"record.{ext}"
            archive_path.rename(final)
            log.info(f"Saved → {final}")

        return record_dir


def download_all_paper_v(output_dir: str = DEFAULT_OUTPUT, fmt: str = "csv"):
    """Download all Paper V-relevant records from HEPData."""
    print(f"\n{'='*70}")
    print(f"  Downloading Paper V-relevant HEPData records")
    print(f"  Format: {fmt}")
    print(f"  Output: {output_dir}")
    print(f"{'='*70}\n")

    manifest = {}

    for key, rec in PAPER_V_RECORDS.items():
        inspire_id = rec["inspire_id"]
        title = rec["title"]
        print(f"{'─'*60}")
        print(f"  {key}: {title}")
        print(f"  INSPIRE: {inspire_id}")
        print(f"  Relevance: {rec['relevance']}")

        try:
            path = download_record(inspire_id, fmt=fmt, output_dir=output_dir)
            manifest[key] = {
                "inspire_id": inspire_id,
                "title": title,
                "path": str(path),
                "format": fmt,
                "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "status": "ok",
            }
            print(f"  ✓ Downloaded\n")
        except Exception as e:
            log.warning(f"  ✗ Failed: {e}")
            manifest[key] = {
                "inspire_id": inspire_id,
                "title": title,
                "status": f"failed: {e}",
            }
            print()

    # Save manifest
    manifest_path = Path(output_dir) / "paper_v_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n{'='*60}")
    print(f"Manifest saved → {manifest_path}")

    ok = sum(1 for v in manifest.values() if v.get("status") == "ok")
    print(f"Downloaded: {ok}/{len(manifest)} records")


# ---------------------------------------------------------------------------
# Catalog / inspection
# ---------------------------------------------------------------------------

def show_catalog(data_dir: str):
    """Show what HEPData records have been downloaded."""
    base = Path(data_dir)
    if not base.exists():
        print(f"No data directory found: {data_dir}")
        return

    print(f"\n{'='*70}")
    print(f"  HEPData Download Catalog — {data_dir}")
    print(f"{'='*70}\n")

    # Check for Paper V manifest
    manifest_path = base / "paper_v_manifest.json"
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)
        for key, rec in manifest.items():
            status = rec.get("status", "?")
            icon = "✓" if status == "ok" else "✗"
            print(f"  {icon} {key}")
            print(f"      {rec.get('title', '?')}")
            print(f"      INSPIRE: {rec.get('inspire_id', '?')}")
            if "path" in rec:
                p = Path(rec["path"])
                if p.is_dir():
                    files = list(p.rglob("*"))
                    data_files = [f for f in files if f.suffix in (".csv", ".json", ".yaml", ".root")]
                    print(f"      Files: {len(data_files)} data files")
            print()
        return

    # Fallback: scan directory
    for d in sorted(base.iterdir()):
        if d.is_dir() and d.name.startswith("ins"):
            files = list(d.rglob("*"))
            data_files = [f for f in files if f.suffix in (".csv", ".json", ".yaml", ".root")]
            total_size = sum(f.stat().st_size for f in files if f.is_file())
            print(f"  {d.name}: {len(data_files)} data files ({_human_size(total_size)})")

    print()


def inspect_csv_tables(data_dir: str, inspire_id: int):
    """Show summary of downloaded CSV tables for a record."""
    if pd is None:
        sys.exit("pandas required: pip install pandas")

    record_dir = Path(data_dir) / f"ins{inspire_id}"
    if not record_dir.exists():
        print(f"No data found for ins{inspire_id}")
        return

    csv_files = sorted(record_dir.rglob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {record_dir}")
        return

    print(f"\n{'='*70}")
    print(f"  CSV Tables for ins{inspire_id}")
    print(f"{'='*70}\n")

    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path)
            print(f"  {csv_path.name}")
            print(f"    Shape: {df.shape}")
            print(f"    Columns: {list(df.columns)}")
            print(f"    Head:")
            print(df.head(5).to_string(index=False))
            print()
        except Exception as e:
            print(f"  {csv_path.name}: Error reading — {e}")
            print()


def show_paper_v_targets():
    """Print the curated list of Paper V-relevant records."""
    print(f"\n{'='*70}")
    print(f"  Paper V — HEPData Targets")
    print(f"{'='*70}\n")

    for key, rec in PAPER_V_RECORDS.items():
        print(f"  {key}")
        print(f"    Title:     {rec['title']}")
        print(f"    Journal:   {rec['journal']}")
        print(f"    INSPIRE:   {rec['inspire_id']}")
        print(f"    Relevance: {rec['relevance']}")
        if rec.get("tables_of_interest"):
            print(f"    Key tables: {', '.join(rec['tables_of_interest'])}")
        print()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="hepdata_downloader",
        description="HEPData Downloader — Published HEP measurement tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show all Paper V-relevant records
  python hepdata_downloader.py targets

  # Download ALL Paper V-relevant records at once
  python hepdata_downloader.py fetch-all --output ./hepdata --format csv

  # Search HEPData
  python hepdata_downloader.py search --query "B0 K* mu mu angular CMS"

  # Download a specific record by INSPIRE ID
  python hepdata_downloader.py fetch --inspire 2850101 --format csv --output ./hepdata

  # Download just one table
  python hepdata_downloader.py fetch --inspire 2850101 --format csv --table "Results P5p"

  # List tables in a record
  python hepdata_downloader.py list-tables --inspire 2850101

  # Inspect downloaded CSV tables
  python hepdata_downloader.py inspect --inspire 2850101 --data-dir ./hepdata

  # Show download catalog
  python hepdata_downloader.py catalog ./hepdata

KEY INSPIRE IDS FOR PAPER V:
  1385600  CMS B0→K*0μμ angular (8 TeV)  — FL, AFB, dB/dq²
  2850101  CMS B0→K*0μμ angular (13 TeV) — P'5, P1-P3 (★ best dataset)
  1826544  CMS B+→K*+μμ angular (8 TeV)  — cross-check channel
  1328493  CMS+LHCb Bs→μμ observation    — BR constrains C10
  2935112  CMS high-mass dilepton+b-jets  — bsll contact interaction limits
  1802523  ATLAS high-mass dilepton       — Z' exclusion / contact interactions
        """,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- targets ---
    sub.add_parser("targets", help="Show Paper V-relevant records")

    # --- search ---
    sp = sub.add_parser("search", help="Search HEPData")
    sp.add_argument("--query", required=True)
    sp.add_argument("--collaboration", help="e.g. CMS, LHCb, ATLAS")
    sp.add_argument("--limit", type=int, default=10)

    # --- fetch ---
    sp = sub.add_parser("fetch", help="Download a specific record")
    sp.add_argument("--inspire", type=int, required=True, help="INSPIRE record ID")
    sp.add_argument("--format", default="csv", choices=["csv", "json", "yaml", "root", "yoda"])
    sp.add_argument("--table", help="Download specific table only")
    sp.add_argument("--output", default=DEFAULT_OUTPUT)

    # --- fetch-all ---
    sp = sub.add_parser("fetch-all", help="Download all Paper V-relevant records")
    sp.add_argument("--output", default=DEFAULT_OUTPUT)
    sp.add_argument("--format", default="csv", choices=["csv", "json", "yaml", "root", "yoda"])

    # --- list-tables ---
    sp = sub.add_parser("list-tables", help="List tables in a record")
    sp.add_argument("--inspire", type=int, required=True)

    # --- inspect ---
    sp = sub.add_parser("inspect", help="Inspect downloaded CSV tables")
    sp.add_argument("--inspire", type=int, required=True)
    sp.add_argument("--data-dir", default=DEFAULT_OUTPUT)

    # --- catalog ---
    sp = sub.add_parser("catalog", help="Show download catalog")
    sp.add_argument("data_dir", default=DEFAULT_OUTPUT, nargs="?")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "targets":
        show_paper_v_targets()

    elif args.command == "search":
        data = search_hepdata(args.query, collaboration=args.collaboration,
                              size=args.limit)
        results = data.get("results", [])
        total = data.get("total", 0)
        print(f"\n{'='*70}")
        print(f"  HEPData Search — {len(results)} of {total} results")
        print(f"{'='*70}\n")
        for r in results:
            inspire_id = ""
            for ident in r.get("identifiers", []):
                if ident.get("type") == "inspire":
                    inspire_id = ident.get("value", "")
            title = r.get("title", "(no title)")
            collab = r.get("collaborations", [])
            year = r.get("year", "?")
            n_tables = r.get("data_tables", 0)
            print(f"  INSPIRE {inspire_id}  [{', '.join(collab)} {year}]")
            print(f"    {title}")
            print(f"    Tables: {n_tables}")
            print()

    elif args.command == "fetch":
        download_record(args.inspire, fmt=args.format, output_dir=args.output,
                        table=args.table)

    elif args.command == "fetch-all":
        download_all_paper_v(output_dir=args.output, fmt=args.format)

    elif args.command == "list-tables":
        tables = list_tables(args.inspire)
        if tables:
            print(f"\nTables in ins{args.inspire}:")
            for i, t in enumerate(tables, 1):
                print(f"  {i}. {t}")
        else:
            print(f"\nCould not retrieve table list. Try:")
            print(f"  python hepdata_downloader.py fetch --inspire {args.inspire} --format json")
            print(f"  Then inspect the JSON to find table names.")

    elif args.command == "inspect":
        inspect_csv_tables(args.data_dir, args.inspire)

    elif args.command == "catalog":
        show_catalog(args.data_dir)


if __name__ == "__main__":
    main()
