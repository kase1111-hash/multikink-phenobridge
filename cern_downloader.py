#!/usr/bin/env python3
"""
CERN Open Data Portal — Bulk Downloader
========================================
Downloads datasets from the CERN Open Data Portal (opendata.cern.ch).
Supports filtering by experiment, file type, collision type, and more.

Usage:
    python cern_downloader.py --help
    python cern_downloader.py search --experiment CMS --type Dataset --limit 20
    python cern_downloader.py download --record 700 --output ./cern_data
    python cern_downloader.py bulk --experiment CMS --type Dataset --limit 5 --output ./cern_data

Requirements:
    pip install requests tqdm
"""

import argparse
import json
import os
import sys
import hashlib
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None
    print("Warning: 'tqdm' not installed. Progress bars disabled. pip install tqdm")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE = "https://opendata.cern.ch/api/records/"
SEARCH_BASE = "https://opendata.cern.ch/api/records/"
# Alternative: the web search endpoint (same facet format, returns HTML unless
# Accept: application/json is set). If /api/records/ doesn't support facets,
# try: SEARCH_BASE = "https://opendata.cern.ch/search"
FILE_BASE = "https://opendata.cern.ch"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("cern_downloader")

VALID_EXPERIMENTS = ["CMS", "ATLAS", "ALICE", "LHCb", "OPERA", "PHENIX"]
VALID_TYPES = ["Dataset", "Software", "Environment", "Documentation", "Supplementaries"]

DEFAULT_OUTPUT = "./cern_data"
DEFAULT_WORKERS = 4
CHUNK_SIZE = 1024 * 1024  # 1 MB
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# ---------------------------------------------------------------------------
# Catalog / manifest helpers
# ---------------------------------------------------------------------------

def save_manifest(output_dir: str, record_id: int, record_meta: dict, files: list):
    """Save a JSON manifest alongside downloaded files for traceability."""
    manifest_path = Path(output_dir) / f"record_{record_id}" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "record_id": record_id,
        "title": record_meta.get("title", ""),
        "experiment": record_meta.get("experiment", ""),
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "files": files,
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    log.info(f"Manifest saved → {manifest_path}")


def load_catalog(output_dir: str) -> dict:
    """Load or create a top-level catalog tracking all downloaded records."""
    catalog_path = Path(output_dir) / "catalog.json"
    if catalog_path.exists():
        with open(catalog_path) as f:
            return json.load(f)
    return {"records": {}, "updated": ""}


def update_catalog(output_dir: str, record_id: int, meta: dict):
    catalog = load_catalog(output_dir)
    catalog["records"][str(record_id)] = {
        "title": meta.get("title", ""),
        "experiment": meta.get("experiment", ""),
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    catalog["updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    catalog_path = Path(output_dir) / "catalog.json"
    with open(catalog_path, "w") as f:
        json.dump(catalog, f, indent=2)

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def search_records(
    experiment: str = None,
    record_type: str = None,
    collision_type: str = None,
    query: str = None,
    limit: int = 20,
    page: int = 1,
) -> dict:
    """
    Search the CERN Open Data Portal.
    Returns raw JSON response with hits.

    The portal uses Invenio with facet-based filtering via 'f=' parameters
    (not Lucene field queries in 'q='). Free-text goes in 'q=',
    structured filters go in separate 'f=' params.

    For title-specific searches, use title.tokens:WORD in q=.
    For wildcard title matches, use title.tokens:*WORD*.
    """
    # Base params
    params = {
        "size": limit,
        "page": page,
        "sort": "mostrecent",
    }

    # Free-text query (supports Lucene syntax: AND, OR, title.tokens:word)
    if query:
        params["q"] = query
    else:
        params["q"] = ""

    # Facet filters go as separate 'f' parameters
    # requests encodes list values as repeated keys: f=val1&f=val2
    facets = []
    if experiment:
        facets.append(f"experiment:{experiment}")
    if record_type:
        facets.append(f"type:{record_type}")
    if collision_type:
        facets.append(f"collision_type:{collision_type}")

    if facets:
        params["f"] = facets

    log.info(f"Searching: {params}")
    resp = requests.get(SEARCH_BASE, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_record(record_id: int) -> dict:
    """Fetch full metadata for a single record."""
    url = f"{API_BASE}{record_id}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def list_record_files(record_id: int) -> list[dict]:
    """Return list of files for a record: [{uri, key, size, checksum}, ...]"""
    data = get_record(record_id)
    metadata = data.get("metadata", {})
    files_list = metadata.get("files", [])
    result = []
    for f in files_list:
        entry = {
            "key": f.get("key", ""),
            "uri": f.get("uri", ""),
            "size": f.get("size", 0),
            "checksum": f.get("checksum", ""),
        }
        result.append(entry)
    return result, metadata


def _verify_checksum(dest: Path, expected_checksum: str) -> bool:
    """Verify file checksum. Returns True if valid."""
    algo, expected_hash = (
        expected_checksum.split(":", 1)
        if ":" in expected_checksum
        else ("md5", expected_checksum)
    )
    h = hashlib.new(algo)
    with open(dest, "rb") as f:
        while True:
            block = f.read(CHUNK_SIZE)
            if not block:
                break
            h.update(block)
    actual = h.hexdigest()
    if actual != expected_hash:
        log.warning(f"Checksum mismatch for {dest.name}: expected {expected_hash}, got {actual}")
        return False
    log.info(f"Checksum verified: {dest.name}")
    return True


def _download_chunk(url: str, start: int, end: int, chunk_path: str,
                    retries: int = MAX_RETRIES) -> bool:
    """Download a single byte-range chunk."""
    for attempt in range(1, retries + 1):
        try:
            headers = {"Range": f"bytes={start}-{end}"}
            resp = requests.get(url, headers=headers, stream=True, timeout=120)
            if resp.status_code not in (200, 206):
                raise Exception(f"HTTP {resp.status_code}")

            with open(chunk_path, "wb") as f:
                for data in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(data)
            return True
        except Exception as e:
            if attempt < retries:
                time.sleep(RETRY_DELAY * attempt)
            else:
                log.error(f"Chunk {start}-{end} failed after {retries} attempts: {e}")
    return False


def _download_parallel(url: str, dest_path: str, num_chunks: int = 8,
                       retries: int = MAX_RETRIES) -> bool:
    """
    Download a file using parallel byte-range requests.
    Splits the file into num_chunks pieces, downloads concurrently,
    then reassembles. Requires server to support Range requests.
    """
    dest = Path(dest_path)

    # First, get file size via HEAD
    try:
        head = requests.head(url, timeout=30, allow_redirects=True)
        total_size = int(head.headers.get("content-length", 0))
        accept_ranges = head.headers.get("accept-ranges", "none")
    except Exception as e:
        log.warning(f"HEAD request failed: {e}")
        return False

    if total_size == 0:
        log.warning("Server didn't return content-length, can't parallelize")
        return False

    if accept_ranges == "none":
        # Try anyway — many servers support ranges without advertising
        log.info("Server doesn't advertise range support, attempting anyway...")

    chunk_size = total_size // num_chunks
    chunks = []
    for i in range(num_chunks):
        start = i * chunk_size
        end = (i + 1) * chunk_size - 1 if i < num_chunks - 1 else total_size - 1
        chunk_path = f"{dest_path}.part{i}"
        chunks.append((start, end, chunk_path))

    log.info(f"Parallel download: {num_chunks} chunks, {_human_size(total_size)} total")

    # Download chunks in parallel
    from concurrent.futures import ThreadPoolExecutor, as_completed

    success = True
    start_time = time.time()

    if tqdm:
        pbar = tqdm(total=total_size, unit="B", unit_scale=True,
                    desc=dest.name[:40])
    else:
        pbar = None

    with ThreadPoolExecutor(max_workers=num_chunks) as executor:
        futures = {}
        for start, end, chunk_path in chunks:
            future = executor.submit(_download_chunk, url, start, end, chunk_path, retries)
            futures[future] = (start, end, chunk_path)

        for future in as_completed(futures):
            start, end, chunk_path = futures[future]
            if future.result():
                actual_size = Path(chunk_path).stat().st_size
                if pbar:
                    pbar.update(actual_size)
            else:
                success = False
                log.error(f"Chunk {start}-{end} failed")

    if pbar:
        pbar.close()

    if not success:
        # Clean up partial chunks
        for _, _, chunk_path in chunks:
            p = Path(chunk_path)
            if p.exists():
                p.unlink()
        return False

    # Reassemble chunks in order
    log.info("Reassembling chunks...")
    with open(dest, "wb") as outf:
        for _, _, chunk_path in chunks:
            cp = Path(chunk_path)
            with open(cp, "rb") as inf:
                while True:
                    block = inf.read(CHUNK_SIZE)
                    if not block:
                        break
                    outf.write(block)
            cp.unlink()

    elapsed = time.time() - start_time
    speed = total_size / elapsed if elapsed > 0 else 0
    log.info(f"Done: {_human_size(total_size)} in {elapsed:.1f}s ({_human_size(int(speed))}/s)")

    # Verify reassembled size
    final_size = dest.stat().st_size
    if final_size != total_size:
        log.error(f"Size mismatch: expected {total_size}, got {final_size}")
        return False

    return True


def download_file(
    uri: str,
    dest_path: str,
    expected_checksum: str = None,
    retries: int = MAX_RETRIES,
    parallel_chunks: int = 0,
) -> bool:
    """Download a single file with retry, resume, and optional checksum verification.

    If parallel_chunks > 0 and the server supports Range requests, downloads
    the file in N parallel byte-range chunks to saturate high-bandwidth connections.
    """
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    url = uri if uri.startswith("http") else f"{FILE_BASE}{uri}"

    # Try parallel chunked download if requested
    if parallel_chunks > 1:
        ok = _download_parallel(url, str(dest), parallel_chunks, retries)
        if ok:
            if expected_checksum:
                return _verify_checksum(dest, expected_checksum)
            return True
        log.warning("Parallel download failed, falling back to sequential")

    for attempt in range(1, retries + 1):
        try:
            # Support resume
            headers = {}
            initial_pos = 0
            if dest.exists():
                initial_pos = dest.stat().st_size
                headers["Range"] = f"bytes={initial_pos}-"

            resp = requests.get(url, headers=headers, stream=True, timeout=60)

            # If server doesn't support range or file is complete
            if resp.status_code == 416:
                log.info(f"Already complete: {dest.name}")
                return True
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0)) + initial_pos
            mode = "ab" if initial_pos > 0 else "wb"

            if tqdm:
                pbar = tqdm(
                    total=total,
                    initial=initial_pos,
                    unit="B",
                    unit_scale=True,
                    desc=dest.name[:40],
                )
            else:
                pbar = None

            with open(dest, mode) as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
                    if pbar:
                        pbar.update(len(chunk))

            if pbar:
                pbar.close()

            # Verify checksum if provided
            if expected_checksum:
                algo, expected_hash = (
                    expected_checksum.split(":", 1)
                    if ":" in expected_checksum
                    else ("md5", expected_checksum)
                )
                h = hashlib.new(algo)
                with open(dest, "rb") as f:
                    while True:
                        block = f.read(CHUNK_SIZE)
                        if not block:
                            break
                        h.update(block)
                actual = h.hexdigest()
                if actual != expected_hash:
                    log.warning(
                        f"Checksum mismatch for {dest.name}: "
                        f"expected {expected_hash}, got {actual}"
                    )
                    dest.unlink()
                    continue  # retry
                else:
                    log.info(f"Checksum verified: {dest.name}")

            return True

        except Exception as e:
            log.warning(f"Attempt {attempt}/{retries} failed for {dest.name}: {e}")
            if attempt < retries:
                time.sleep(RETRY_DELAY * attempt)

    log.error(f"Failed to download {dest.name} after {retries} attempts")
    return False

# ---------------------------------------------------------------------------
# High-level commands
# ---------------------------------------------------------------------------

def cmd_search(args):
    """Search and display matching records."""
    data = search_records(
        experiment=args.experiment,
        record_type=args.type,
        collision_type=args.collision,
        query=args.query,
        limit=args.limit,
        page=args.page,
    )

    hits = data.get("hits", {}).get("hits", [])
    total = data.get("hits", {}).get("total", 0)
    print(f"\n{'='*80}")
    print(f"  CERN Open Data — Search Results  ({len(hits)} of {total} total)")
    print(f"{'='*80}\n")

    for h in hits:
        meta = h.get("metadata", {})
        rec_id = h.get("id", "?")
        title = meta.get("title", "(no title)")
        experiment = meta.get("experiment", "(unknown)")
        n_files = len(meta.get("files", []))
        total_size = sum(f.get("size", 0) for f in meta.get("files", []))
        size_str = _human_size(total_size)

        print(f"  Record {rec_id}")
        print(f"    Title:      {title}")
        print(f"    Experiment: {experiment}")
        print(f"    Files:      {n_files}  ({size_str})")
        print()

    if args.save:
        out = Path(args.save)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Raw results saved → {out}")


def cmd_list(args):
    """List files in a specific record."""
    files, meta = list_record_files(args.record)
    title = meta.get("title", "(no title)")
    print(f"\n{'='*80}")
    print(f"  Record {args.record}: {title}")
    print(f"  {len(files)} file(s)")
    print(f"{'='*80}\n")

    for f in files:
        size_str = _human_size(f["size"])
        print(f"  {f['key']:<60s}  {size_str:>10s}")
    print()


def cmd_download(args):
    """Download all files for a single record."""
    files, meta = list_record_files(args.record)
    title = meta.get("title", "(no title)")
    record_dir = Path(args.output) / f"record_{args.record}"

    # Filter by extension if specified
    if args.ext:
        exts = [e.lower().lstrip(".") for e in args.ext.split(",")]
        files = [f for f in files if any(f["key"].lower().endswith(f".{e}") for e in exts)]

    total_size = sum(f["size"] for f in files)
    print(f"\nDownloading record {args.record}: {title}")
    print(f"  {len(files)} file(s), {_human_size(total_size)} total")
    print(f"  Destination: {record_dir}\n")

    if args.dry_run:
        for f in files:
            print(f"  [DRY RUN] Would download: {f['key']} ({_human_size(f['size'])})")
        return

    downloaded = []
    for f in files:
        dest = record_dir / f["key"]
        ok = download_file(f["uri"], str(dest), f.get("checksum"),
                           parallel_chunks=getattr(args, "parallel", 0))
        downloaded.append({**f, "downloaded": ok, "local_path": str(dest)})

    save_manifest(args.output, args.record, meta, downloaded)
    update_catalog(args.output, args.record, meta)

    success = sum(1 for d in downloaded if d["downloaded"])
    print(f"\nDone: {success}/{len(downloaded)} files downloaded.")


def cmd_bulk(args):
    """Search + download multiple records."""
    data = search_records(
        experiment=args.experiment,
        record_type=args.type,
        collision_type=args.collision,
        query=args.query,
        limit=args.limit,
    )
    hits = data.get("hits", {}).get("hits", [])
    print(f"\nBulk download: {len(hits)} record(s) matched.\n")

    for h in hits:
        rec_id = h.get("id")
        meta = h.get("metadata", {})
        title = meta.get("title", "(no title)")
        print(f"{'─'*60}")
        print(f"Record {rec_id}: {title}")

        files, _ = list_record_files(rec_id)
        if args.ext:
            exts = [e.lower().lstrip(".") for e in args.ext.split(",")]
            files = [f for f in files if any(f["key"].lower().endswith(f".{e}") for e in exts)]

        if args.dry_run:
            for f in files:
                print(f"  [DRY RUN] {f['key']} ({_human_size(f['size'])})")
            continue

        downloaded = []
        for f in files:
            dest = Path(args.output) / f"record_{rec_id}" / f["key"]
            ok = download_file(f["uri"], str(dest), f.get("checksum"))
            downloaded.append({**f, "downloaded": ok, "local_path": str(dest)})

        save_manifest(args.output, rec_id, meta, downloaded)
        update_catalog(args.output, rec_id, meta)

    print(f"\n{'='*60}")
    print("Bulk download complete.")


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

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cern_downloader",
        description="CERN Open Data Portal — Bulk Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for CMS datasets (uses facet filters, not Lucene field queries)
  python cern_downloader.py search --experiment CMS --type Dataset --limit 10

  # Search by title keywords (use title.tokens: for exact word match)
  python cern_downloader.py search --query "title.tokens:*muon*" --experiment CMS

  # Search for dimuon or NanoAOD datasets
  python cern_downloader.py search --query "dimuon" --experiment CMS
  python cern_downloader.py search --query "NanoAOD muon" --experiment CMS --type Dataset

  # List files in a specific record
  python cern_downloader.py list --record 12341

  # Download a single record (only .root files)
  python cern_downloader.py download --record 12341 --ext root --output ./cern_data

  # Direct download of the CMS 2012 dimuon NanoAOD (2.1 GiB, 61.5M events)
  python cern_downloader.py direct \\
    --url http://opendata.cern.ch/record/12341/files/Run2012BC_DoubleMuParked_Muons.root \\
    --output ./cern_data

  # Same thing but with 8 parallel chunks (saturate a fast connection)
  python cern_downloader.py direct \\
    --url http://opendata.cern.ch/record/12341/files/Run2012BC_DoubleMuParked_Muons.root \\
    --output ./cern_data --parallel 8

  # Bulk download first 5 CMS datasets (dry run)
  python cern_downloader.py bulk --experiment CMS --type Dataset --limit 5 --dry-run

KEY RECORD IDS FOR PHYSICS ANALYSIS:
  12341  DoubleMuParked 2012 NanoAOD (muons only, 61.5M events, 2.1 GiB) ★
  12342  Official dimuon spectrum analysis code for record 12341
  12365  Run2012B DoubleMuParked NanoAOD (education/outreach)
  6004   DoubleMuParked Run2012B primary AOD (requires CMSSW)
  6030   DoubleMuParked Run2012C primary AOD (requires CMSSW)
  30560  MuOnia Run2016H NanoAOD (full branches, no CMSSW needed)
  545    Derived 2011 datasets (CSV format, simpler)
  5001   Example dimuon analysis code (CMSSW-based)
        """,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- search ---
    sp = sub.add_parser("search", help="Search the portal")
    sp.add_argument("--experiment", choices=VALID_EXPERIMENTS)
    sp.add_argument("--type", choices=VALID_TYPES)
    sp.add_argument("--collision", help="Collision type, e.g. pp, PbPb")
    sp.add_argument("--query", help="Free-text search query")
    sp.add_argument("--limit", type=int, default=20)
    sp.add_argument("--page", type=int, default=1)
    sp.add_argument("--save", help="Save raw JSON results to file")

    # --- list ---
    sp = sub.add_parser("list", help="List files in a record")
    sp.add_argument("--record", type=int, required=True)

    # --- download ---
    sp = sub.add_parser("download", help="Download files from a single record")
    sp.add_argument("--record", type=int, required=True)
    sp.add_argument("--output", default=DEFAULT_OUTPUT)
    sp.add_argument("--ext", help="Comma-separated file extensions to filter, e.g. root,csv")
    sp.add_argument("--dry-run", action="store_true")
    sp.add_argument("--parallel", type=int, default=0,
                    help="Number of parallel chunks (0=sequential, 8-16 for fast connections)")

    # --- bulk ---
    sp = sub.add_parser("bulk", help="Search + download multiple records")
    sp.add_argument("--experiment", choices=VALID_EXPERIMENTS)
    sp.add_argument("--type", choices=VALID_TYPES)
    sp.add_argument("--collision", help="Collision type")
    sp.add_argument("--query", help="Free-text search query")
    sp.add_argument("--limit", type=int, default=10)
    sp.add_argument("--output", default=DEFAULT_OUTPUT)
    sp.add_argument("--ext", help="Comma-separated file extensions to filter")
    sp.add_argument("--dry-run", action="store_true")

    # --- direct ---
    sp = sub.add_parser("direct", help="Download a file directly by URL")
    sp.add_argument("--url", required=True, help="Direct HTTP/HTTPS URL to file")
    sp.add_argument("--output", default=DEFAULT_OUTPUT, help="Output directory")
    sp.add_argument("--filename", help="Override output filename")
    sp.add_argument("--parallel", type=int, default=0,
                    help="Number of parallel chunks (0=sequential, 8-16 for fast connections)")

    return parser


def cmd_direct(args):
    """Download a single file directly by URL."""
    url = args.url
    fname = args.filename or url.split("/")[-1]
    dest = Path(args.output) / fname
    print(f"\nDirect download: {url}")
    print(f"  Destination: {dest}")
    if args.parallel > 0:
        print(f"  Parallel chunks: {args.parallel}")
    print()
    ok = download_file(url, str(dest), parallel_chunks=args.parallel)
    if ok:
        print(f"\nDone: {dest}  ({_human_size(dest.stat().st_size)})")
    else:
        print(f"\nFailed to download {fname}")


def main():
    parser = build_parser()
    args = parser.parse_args()

    commands = {
        "search": cmd_search,
        "list": cmd_list,
        "download": cmd_download,
        "bulk": cmd_bulk,
        "direct": cmd_direct,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
