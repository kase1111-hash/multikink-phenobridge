#!/usr/bin/env python3
"""
CERN Open Data — Analyzer
==========================
Analyzes datasets downloaded by cern_downloader.py.
Supports ROOT files (via uproot), CSV/JSON, and common HEP analysis patterns:
  - Invariant mass reconstruction
  - Histogram generation
  - Cut-flow analysis
  - Event-level statistics
  - Batch processing across records

Usage:
    python cern_analyzer.py --help
    python cern_analyzer.py catalog ./cern_data
    python cern_analyzer.py inspect ./cern_data/record_700/file.root
    python cern_analyzer.py histogram ./cern_data/record_700/file.root --branch InvariantMass --bins 100
    python cern_analyzer.py stats ./cern_data/record_700/file.root --branches pt eta phi
    python cern_analyzer.py invariant-mass ./cern_data/record_700/file.root --particles muon
    python cern_analyzer.py scan ./cern_data --ext root --report summary.json

Requirements:
    pip install uproot awkward numpy matplotlib pandas
    (Optional: pip install hist mplhep for HEP-style plots)
"""

import argparse
import json
import os
import sys
import glob
import logging
from pathlib import Path
from collections import defaultdict

import numpy as np

try:
    import uproot
except ImportError:
    uproot = None

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

try:
    import awkward as ak
except ImportError:
    ak = None

# Optional HEP-style plotting
try:
    import mplhep

    if plt:
        plt.style.use(mplhep.style.CMS)
    HAS_MPLHEP = True
except ImportError:
    HAS_MPLHEP = False

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("cern_analyzer")

# ---------------------------------------------------------------------------
# File loading helpers
# ---------------------------------------------------------------------------

def load_root_file(filepath: str) -> "uproot.ReadOnlyDirectory":
    """Open a ROOT file and return the uproot handle."""
    if uproot is None:
        sys.exit("uproot is required for ROOT files: pip install uproot awkward")
    return uproot.open(filepath)


def load_csv_file(filepath: str) -> "pd.DataFrame":
    if pd is None:
        sys.exit("pandas is required for CSV files: pip install pandas")
    return pd.read_csv(filepath)


def load_json_file(filepath: str):
    with open(filepath) as f:
        return json.load(f)


def load_file(filepath: str):
    """Auto-detect and load a data file."""
    p = Path(filepath)
    ext = p.suffix.lower()
    if ext == ".root":
        return ("root", load_root_file(filepath))
    elif ext == ".csv":
        return ("csv", load_csv_file(filepath))
    elif ext == ".json":
        return ("json", load_json_file(filepath))
    else:
        log.warning(f"Unknown extension '{ext}', attempting as ROOT...")
        try:
            return ("root", load_root_file(filepath))
        except Exception:
            sys.exit(f"Cannot load file: {filepath}")


# ---------------------------------------------------------------------------
# ROOT inspection
# ---------------------------------------------------------------------------

def inspect_root(filepath: str, max_depth: int = 3):
    """Print the structure of a ROOT file: trees, branches, types."""
    f = load_root_file(filepath)
    print(f"\n{'='*70}")
    print(f"  ROOT File: {Path(filepath).name}")
    print(f"{'='*70}\n")

    def _walk(obj, prefix="", depth=0):
        if depth > max_depth:
            return
        try:
            keys = obj.keys()
        except Exception:
            return

        for key in keys:
            child = obj[key]
            classname = type(child).__name__
            indent = "  " * depth

            if hasattr(child, "num_entries"):
                n = child.num_entries
                print(f"{indent}{prefix}{key}  [{classname}, {n:,} entries]")
                # Show branches
                if hasattr(child, "keys"):
                    for bname in child.keys():
                        branch = child[bname]
                        typename = ""
                        try:
                            typename = branch.typename
                        except Exception:
                            pass
                        interp = ""
                        try:
                            interp = str(branch.interpretation)
                        except Exception:
                            pass
                        print(f"{indent}    ├─ {bname}  ({typename})  [{interp}]")
            else:
                print(f"{indent}{prefix}{key}  [{classname}]")
                _walk(child, prefix=f"{key}/", depth=depth + 1)

    _walk(f)
    print()


# ---------------------------------------------------------------------------
# Tree / branch data extraction
# ---------------------------------------------------------------------------

def get_tree(filepath: str, tree_name: str = None):
    """Get a TTree from a ROOT file. Auto-selects if only one tree exists."""
    f = load_root_file(filepath)

    # Find all TTrees
    trees = {}
    for key in f.keys():
        obj = f[key]
        if hasattr(obj, "num_entries"):
            trees[key.split(";")[0]] = obj

    if not trees:
        sys.exit("No TTrees found in file.")

    if tree_name:
        if tree_name in trees:
            return trees[tree_name]
        # Try partial match
        matches = [k for k in trees if tree_name in k]
        if len(matches) == 1:
            return trees[matches[0]]
        sys.exit(f"Tree '{tree_name}' not found. Available: {list(trees.keys())}")

    if len(trees) == 1:
        name, tree = next(iter(trees.items()))
        log.info(f"Auto-selected tree: {name}")
        return tree

    print("Multiple trees found. Please specify --tree:")
    for name, tree in trees.items():
        print(f"  {name}  ({tree.num_entries:,} entries)")
    sys.exit()


def extract_branches(filepath: str, branches: list, tree_name: str = None,
                     entry_start: int = None, entry_stop: int = None) -> dict:
    """Extract branch data as numpy/awkward arrays."""
    tree = get_tree(filepath, tree_name)
    available = tree.keys()

    # Validate branches
    missing = [b for b in branches if b not in available]
    if missing:
        log.warning(f"Missing branches: {missing}")
        log.info(f"Available: {available[:20]}{'...' if len(available) > 20 else ''}")
        branches = [b for b in branches if b in available]

    if not branches:
        sys.exit("No valid branches to extract.")

    arrays = tree.arrays(branches, entry_start=entry_start, entry_stop=entry_stop,
                         library="np" if ak is None else "ak")
    return {b: arrays[b] for b in branches}


# ---------------------------------------------------------------------------
# Analysis commands
# ---------------------------------------------------------------------------

def cmd_catalog(args):
    """Show what's been downloaded — reads catalog.json and manifests."""
    catalog_path = Path(args.data_dir) / "catalog.json"
    if not catalog_path.exists():
        print("No catalog.json found. Have you downloaded anything yet?")
        return

    with open(catalog_path) as f:
        catalog = json.load(f)

    records = catalog.get("records", {})
    print(f"\n{'='*70}")
    print(f"  Downloaded Data Catalog — {len(records)} record(s)")
    print(f"  Last updated: {catalog.get('updated', '?')}")
    print(f"{'='*70}\n")

    total_files = 0
    total_bytes = 0

    for rec_id, meta in sorted(records.items(), key=lambda x: int(x[0])):
        rec_dir = Path(args.data_dir) / f"record_{rec_id}"
        manifest_path = rec_dir / "manifest.json"

        n_files = 0
        rec_size = 0
        if manifest_path.exists():
            with open(manifest_path) as f:
                manifest = json.load(f)
            files = manifest.get("files", [])
            n_files = sum(1 for fl in files if fl.get("downloaded"))
            rec_size = sum(fl.get("size", 0) for fl in files if fl.get("downloaded"))

        total_files += n_files
        total_bytes += rec_size

        print(f"  Record {rec_id}: {meta.get('title', '?')}")
        print(f"    Experiment: {meta.get('experiment', '?')}")
        print(f"    Files: {n_files}  Size: {_human_size(rec_size)}")
        print()

    print(f"  Total: {total_files} files, {_human_size(total_bytes)}")
    print()


def cmd_inspect(args):
    """Inspect file structure."""
    p = Path(args.file)
    if p.suffix.lower() == ".root":
        inspect_root(args.file, max_depth=args.depth)
    elif p.suffix.lower() == ".csv":
        df = load_csv_file(args.file)
        print(f"\nCSV: {p.name}")
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        print(f"\n  Head:\n{df.head(10).to_string()}")
        print(f"\n  Describe:\n{df.describe().to_string()}")
    elif p.suffix.lower() == ".json":
        data = load_json_file(args.file)
        if isinstance(data, list):
            print(f"\nJSON array with {len(data)} items")
            if data:
                print(f"  First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else type(data[0])}")
        elif isinstance(data, dict):
            print(f"\nJSON object with keys: {list(data.keys())[:20]}")
    else:
        print(f"Unsupported format: {p.suffix}")


def cmd_stats(args):
    """Compute summary statistics for selected branches."""
    branches = args.branches
    data = extract_branches(args.file, branches, tree_name=args.tree,
                            entry_start=args.start, entry_stop=args.stop)

    print(f"\n{'='*70}")
    print(f"  Branch Statistics — {Path(args.file).name}")
    print(f"{'='*70}\n")

    results = {}
    for bname, arr in data.items():
        # Flatten if jagged
        flat = _flatten(arr)
        if len(flat) == 0:
            print(f"  {bname}: (empty)")
            continue

        stats = {
            "count": len(flat),
            "mean": float(np.mean(flat)),
            "std": float(np.std(flat)),
            "min": float(np.min(flat)),
            "q25": float(np.percentile(flat, 25)),
            "median": float(np.median(flat)),
            "q75": float(np.percentile(flat, 75)),
            "max": float(np.max(flat)),
        }
        results[bname] = stats

        print(f"  {bname}:")
        print(f"    count  = {stats['count']:>12,}")
        print(f"    mean   = {stats['mean']:>12.4f}")
        print(f"    std    = {stats['std']:>12.4f}")
        print(f"    min    = {stats['min']:>12.4f}")
        print(f"    25%    = {stats['q25']:>12.4f}")
        print(f"    50%    = {stats['median']:>12.4f}")
        print(f"    75%    = {stats['q75']:>12.4f}")
        print(f"    max    = {stats['max']:>12.4f}")
        print()

    if args.save:
        with open(args.save, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Stats saved → {args.save}")


def cmd_histogram(args):
    """Generate histograms for a branch."""
    if plt is None:
        sys.exit("matplotlib is required: pip install matplotlib")

    data = extract_branches(args.file, [args.branch], tree_name=args.tree,
                            entry_start=args.start, entry_stop=args.stop)
    arr = _flatten(data[args.branch])

    # Apply range
    if args.xmin is not None:
        arr = arr[arr >= args.xmin]
    if args.xmax is not None:
        arr = arr[arr <= args.xmax]

    fig, ax = plt.subplots(figsize=(10, 6))
    counts, bin_edges, _ = ax.hist(
        arr, bins=args.bins,
        range=(args.xmin, args.xmax) if args.xmin is not None else None,
        histtype="stepfilled", alpha=0.7, edgecolor="black", linewidth=0.5,
        color="#4A90D9",
    )
    ax.set_xlabel(args.branch, fontsize=14)
    ax.set_ylabel("Events", fontsize=14)
    ax.set_title(f"{args.branch} — {Path(args.file).name}", fontsize=14)

    if args.log:
        ax.set_yscale("log")

    ax.text(0.98, 0.97, f"Entries: {len(arr):,}\nMean: {np.mean(arr):.4f}\nStd: {np.std(arr):.4f}",
            transform=ax.transAxes, ha="right", va="top",
            fontsize=10, bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8))

    out_path = args.output or f"hist_{args.branch}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Histogram saved → {out_path}")

    # Also save bin data
    if args.save_bins:
        bin_data = {
            "branch": args.branch,
            "entries": int(len(arr)),
            "bin_edges": bin_edges.tolist(),
            "counts": counts.tolist(),
        }
        with open(args.save_bins, "w") as f:
            json.dump(bin_data, f, indent=2)
        print(f"Bin data saved → {args.save_bins}")


def cmd_invariant_mass(args):
    """
    Reconstruct invariant mass from particle 4-vectors.
    Expects branches like: pt, eta, phi, (mass or energy).
    Handles both dimuon and dielectron cases.
    """
    if plt is None:
        sys.exit("matplotlib is required: pip install matplotlib")

    particle = args.particles.lower()

    # Common CMS Open Data branch naming patterns
    branch_maps = {
        "muon": {
            "pt": ["Muon_Pt", "Muon_pt", "muon_pt", "Pt", "pt1", "pt"],
            "eta": ["Muon_Eta", "Muon_eta", "muon_eta", "Eta", "eta1", "eta"],
            "phi": ["Muon_Phi", "Muon_phi", "muon_phi", "Phi", "phi1", "phi"],
            "mass": ["Muon_Mass", "Muon_mass", "muon_mass"],
            "charge": ["Muon_Charge", "Muon_charge", "muon_charge", "Q1", "charge"],
            "default_mass": 0.10566,  # muon mass in GeV
        },
        "electron": {
            "pt": ["Electron_Pt", "Electron_pt", "electron_pt", "Pt", "pt1"],
            "eta": ["Electron_Eta", "Electron_eta", "electron_eta", "Eta", "eta1"],
            "phi": ["Electron_Phi", "Electron_phi", "electron_phi", "Phi", "phi1"],
            "mass": ["Electron_Mass", "Electron_mass", "electron_mass"],
            "charge": ["Electron_Charge", "Electron_charge", "electron_charge"],
            "default_mass": 0.000511,  # electron mass in GeV
        },
    }

    if particle not in branch_maps:
        sys.exit(f"Unsupported particle: {particle}. Supported: {list(branch_maps.keys())}")

    bmap = branch_maps[particle]
    tree = get_tree(args.file, args.tree)
    available = tree.keys()

    def _find_branch(candidates):
        for c in candidates:
            if c in available:
                return c
        return None

    pt_br = _find_branch(bmap["pt"])
    eta_br = _find_branch(bmap["eta"])
    phi_br = _find_branch(bmap["phi"])
    mass_br = _find_branch(bmap.get("mass", []))
    charge_br = _find_branch(bmap.get("charge", []))

    if not all([pt_br, eta_br, phi_br]):
        print(f"Could not find required branches for {particle}.")
        print(f"  pt:     {pt_br or 'NOT FOUND'}")
        print(f"  eta:    {eta_br or 'NOT FOUND'}")
        print(f"  phi:    {phi_br or 'NOT FOUND'}")
        print(f"  mass:   {mass_br or '(using default)'}")
        print(f"  charge: {charge_br or '(not used)'}")
        print(f"\nAvailable branches: {available}")

        if args.pt and args.eta and args.phi:
            pt_br, eta_br, phi_br = args.pt, args.eta, args.phi
            mass_br = args.mass_branch
            print(f"\nUsing manual overrides: pt={pt_br}, eta={eta_br}, phi={phi_br}")
        else:
            sys.exit("Use --pt, --eta, --phi to specify branches manually.")

    # Extract data
    branch_list = [pt_br, eta_br, phi_br]
    if mass_br:
        branch_list.append(mass_br)
    if charge_br:
        branch_list.append(charge_br)

    arrays = tree.arrays(branch_list, entry_start=args.start, entry_stop=args.stop,
                         library="np" if ak is None else "ak")

    pt = arrays[pt_br]
    eta = arrays[eta_br]
    phi = arrays[phi_br]
    m = arrays[mass_br] if mass_br else bmap["default_mass"]

    # For jagged arrays (multiple particles per event), compute diparticle mass
    charge = arrays[charge_br] if charge_br else None
    inv_masses = _compute_invariant_mass(pt, eta, phi, m, charge=charge)

    if len(inv_masses) == 0:
        sys.exit("No valid diparticle pairs found.")

    # Default range around Z mass for muons/electrons
    xmin = args.xmin or 0
    xmax = args.xmax or 200

    mask = (inv_masses >= xmin) & (inv_masses <= xmax)
    inv_masses_cut = inv_masses[mask]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(inv_masses_cut, bins=args.bins, histtype="stepfilled",
            alpha=0.7, edgecolor="black", linewidth=0.5, color="#D94A4A")
    ax.set_xlabel(f"M({particle}{particle}) [GeV]", fontsize=14)
    ax.set_ylabel("Events", fontsize=14)
    ax.set_title(f"Invariant Mass Spectrum — {particle}s", fontsize=14)

    if args.log:
        ax.set_yscale("log")

    # Annotate peaks
    peak_idx = np.argmax(np.histogram(inv_masses_cut, bins=args.bins)[0])
    bin_edges = np.histogram(inv_masses_cut, bins=args.bins)[1]
    peak_mass = (bin_edges[peak_idx] + bin_edges[peak_idx + 1]) / 2
    ax.axvline(peak_mass, color="red", linestyle="--", alpha=0.6)
    ax.text(0.98, 0.97,
            f"Entries: {len(inv_masses_cut):,}\nPeak: {peak_mass:.2f} GeV\nMean: {np.mean(inv_masses_cut):.2f} GeV",
            transform=ax.transAxes, ha="right", va="top",
            fontsize=10, bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))

    out_path = args.output or f"invariant_mass_{particle}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Invariant mass plot saved → {out_path}")
    print(f"  Events in range [{xmin}, {xmax}] GeV: {len(inv_masses_cut):,}")
    print(f"  Peak mass: {peak_mass:.2f} GeV")


def cmd_cutflow(args):
    """
    Apply sequential cuts and report event yields.
    Cuts are specified as 'branch_name>value' or 'branch_name<value'.
    """
    cuts = []
    for c in args.cuts:
        for op in [">=", "<=", "!=", ">", "<", "=="]:
            if op in c:
                branch, val = c.split(op, 1)
                cuts.append((branch.strip(), op, float(val.strip())))
                break
        else:
            sys.exit(f"Invalid cut syntax: '{c}'. Use e.g. 'pt>20' or 'eta<2.4'")

    all_branches = list(set(b for b, _, _ in cuts))
    data = extract_branches(args.file, all_branches, tree_name=args.tree,
                            entry_start=args.start, entry_stop=args.stop)

    # For jagged arrays, check per-event (any particle passes)
    n_total = None
    mask = None

    print(f"\n{'='*60}")
    print(f"  Cut-flow — {Path(args.file).name}")
    print(f"{'='*60}\n")
    print(f"  {'Cut':<35s} {'Remaining':>12s} {'Efficiency':>10s} {'Cumulative':>10s}")
    print(f"  {'─'*35} {'─'*12} {'─'*10} {'─'*10}")

    for i, (branch, op, val) in enumerate(cuts):
        arr = data[branch]
        flat = _flatten(arr)

        if n_total is None:
            n_total = len(flat)
            mask = np.ones(len(flat), dtype=bool)
            print(f"  {'Initial':<35s} {n_total:>12,}")

        op_func = {
            ">": np.greater, "<": np.less,
            ">=": np.greater_equal, "<=": np.less_equal,
            "==": np.equal, "!=": np.not_equal,
        }[op]

        this_cut = op_func(flat, val)
        mask = mask & this_cut[:len(mask)]
        remaining = int(np.sum(mask))
        step_eff = remaining / max(1, (n_total if i == 0 else int(np.sum(mask | ~this_cut[:len(mask)]))))
        cum_eff = remaining / max(1, n_total)

        label = f"{branch} {op} {val}"
        print(f"  {label:<35s} {remaining:>12,} {step_eff:>9.1%} {cum_eff:>10.1%}")

    print()


def cmd_scan(args):
    """Scan all files in a directory tree and produce a summary report."""
    data_dir = Path(args.data_dir)
    extensions = [e.lower().lstrip(".") for e in args.ext.split(",")] if args.ext else ["root", "csv", "json"]

    files_found = []
    for ext in extensions:
        files_found.extend(data_dir.rglob(f"*.{ext}"))

    print(f"\nScanning {data_dir}: found {len(files_found)} files\n")

    report = {"data_dir": str(data_dir), "files": []}

    for fp in sorted(files_found):
        entry = {"path": str(fp), "size": fp.stat().st_size, "type": fp.suffix}

        if fp.suffix.lower() == ".root" and uproot is not None:
            try:
                f = uproot.open(str(fp))
                trees = {}
                for key in f.keys():
                    obj = f[key]
                    if hasattr(obj, "num_entries"):
                        tname = key.split(";")[0]
                        trees[tname] = {
                            "entries": obj.num_entries,
                            "branches": list(obj.keys()),
                        }
                entry["trees"] = trees
            except Exception as e:
                entry["error"] = str(e)

        elif fp.suffix.lower() == ".csv" and pd is not None:
            try:
                df = pd.read_csv(str(fp), nrows=5)
                entry["columns"] = list(df.columns)
                entry["rows"] = len(pd.read_csv(str(fp)))
            except Exception as e:
                entry["error"] = str(e)

        report["files"].append(entry)
        status = "OK" if "error" not in entry else f"ERR: {entry['error']}"
        print(f"  {fp.relative_to(data_dir)}  ({_human_size(entry['size'])})  [{status}]")

    out = args.report or "scan_report.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport saved → {out}")


def cmd_compare(args):
    """Compare a branch distribution across two files (overlay histograms)."""
    if plt is None:
        sys.exit("matplotlib is required: pip install matplotlib")

    d1 = extract_branches(args.file1, [args.branch], tree_name=args.tree)
    d2 = extract_branches(args.file2, [args.branch], tree_name=args.tree)

    a1 = _flatten(d1[args.branch])
    a2 = _flatten(d2[args.branch])

    fig, ax = plt.subplots(figsize=(10, 6))
    range_ = (
        min(np.min(a1), np.min(a2)),
        max(np.max(a1), np.max(a2)),
    )
    ax.hist(a1, bins=args.bins, range=range_, histtype="step",
            linewidth=2, label=Path(args.file1).stem, density=args.normalize)
    ax.hist(a2, bins=args.bins, range=range_, histtype="step",
            linewidth=2, label=Path(args.file2).stem, density=args.normalize)
    ax.set_xlabel(args.branch, fontsize=14)
    ax.set_ylabel("Density" if args.normalize else "Events", fontsize=14)
    ax.set_title(f"Comparison: {args.branch}", fontsize=14)
    ax.legend()

    if args.log:
        ax.set_yscale("log")

    out_path = args.output or f"compare_{args.branch}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Comparison plot saved → {out_path}")


# ---------------------------------------------------------------------------
# Physics helpers
# ---------------------------------------------------------------------------

def _compute_invariant_mass(pt, eta, phi, mass, charge=None):
    """
    Compute diparticle invariant mass from pt, eta, phi arrays.
    For jagged awkward arrays: selects events with >=2 particles,
    takes first two, optionally filters opposite-sign pairs.
    Stays in awkward-land to avoid memory blowup on large datasets.
    """
    if ak is not None and isinstance(pt, ak.Array):
        return _compute_invariant_mass_ak(pt, eta, phi, mass, charge)

    # Fallback: numpy path for non-awkward arrays
    pt_np = np.asarray(pt)
    eta_np = np.asarray(eta)
    phi_np = np.asarray(phi)
    if isinstance(mass, (int, float)):
        m_np = np.full_like(pt_np, mass)
    else:
        m_np = np.asarray(mass)

    if pt_np.ndim == 2 and pt_np.shape[1] >= 2:
        pt1, pt2 = pt_np[:, 0], pt_np[:, 1]
        eta1, eta2 = eta_np[:, 0], eta_np[:, 1]
        phi1, phi2 = phi_np[:, 0], phi_np[:, 1]
        m1, m2 = m_np[:, 0], m_np[:, 1]
    elif pt_np.ndim == 1:
        n = len(pt_np) - (len(pt_np) % 2)
        pt1, pt2 = pt_np[:n:2], pt_np[1:n:2]
        eta1, eta2 = eta_np[:n:2], eta_np[1:n:2]
        phi1, phi2 = phi_np[:n:2], phi_np[1:n:2]
        m1, m2 = m_np[:n:2], m_np[1:n:2]
    else:
        return np.array([])

    return _mass_from_components(pt1, pt2, eta1, eta2, phi1, phi2, m1, m2)


def _compute_invariant_mass_ak(pt, eta, phi, mass, charge):
    """Awkward-native invariant mass computation. Memory-efficient."""
    import awkward as ak

    # Count muons per event
    nmu = ak.num(pt)

    # Select events with at least 2 muons
    has2 = nmu >= 2
    pt = pt[has2]
    eta = eta[has2]
    phi = phi[has2]
    charge = charge[has2] if charge is not None else None

    if isinstance(mass, (int, float)):
        default_mass = mass
        use_default_mass = True
    else:
        mass = mass[has2]
        use_default_mass = False

    n_events = len(pt)
    log.info(f"Events with >=2 muons: {n_events:,} / {int(ak.sum(nmu > 0)):,}")

    # Take first two muons per event using slicing
    pt1 = pt[:, 0]
    pt2 = pt[:, 1]
    eta1 = eta[:, 0]
    eta2 = eta[:, 1]
    phi1 = phi[:, 0]
    phi2 = phi[:, 1]

    if use_default_mass:
        m1 = default_mass
        m2 = default_mass
    else:
        m1 = mass[:, 0]
        m2 = mass[:, 1]

    # Opposite-sign filter
    if charge is not None:
        q1 = charge[:, 0]
        q2 = charge[:, 1]
        opp_sign = (q1 * q2) < 0
        n_opp = int(ak.sum(opp_sign))
        log.info(f"Opposite-sign filter: {n_opp:,} / {n_events:,} pairs ({100*n_opp/max(n_events,1):.1f}%)")

        pt1 = pt1[opp_sign]
        pt2 = pt2[opp_sign]
        eta1 = eta1[opp_sign]
        eta2 = eta2[opp_sign]
        phi1 = phi1[opp_sign]
        phi2 = phi2[opp_sign]
        if not use_default_mass:
            m1 = m1[opp_sign]
            m2 = m2[opp_sign]

    # Convert to numpy for the four-vector math (now much smaller arrays)
    pt1 = ak.to_numpy(pt1).astype(np.float64)
    pt2 = ak.to_numpy(pt2).astype(np.float64)
    eta1 = ak.to_numpy(eta1).astype(np.float64)
    eta2 = ak.to_numpy(eta2).astype(np.float64)
    phi1 = ak.to_numpy(phi1).astype(np.float64)
    phi2 = ak.to_numpy(phi2).astype(np.float64)

    if use_default_mass:
        m1_np = np.full_like(pt1, default_mass)
        m2_np = np.full_like(pt2, default_mass)
    else:
        m1_np = ak.to_numpy(m1).astype(np.float64)
        m2_np = ak.to_numpy(m2).astype(np.float64)

    log.info(f"Computing invariant mass for {len(pt1):,} pairs...")
    return _mass_from_components(pt1, pt2, eta1, eta2, phi1, phi2, m1_np, m2_np)


def _mass_from_components(pt1, pt2, eta1, eta2, phi1, phi2, m1, m2):
    """Compute invariant mass from two particles' kinematic components."""
    px1 = pt1 * np.cos(phi1)
    py1 = pt1 * np.sin(phi1)
    pz1 = pt1 * np.sinh(eta1)
    E1 = np.sqrt(px1**2 + py1**2 + pz1**2 + m1**2)

    px2 = pt2 * np.cos(phi2)
    py2 = pt2 * np.sin(phi2)
    pz2 = pt2 * np.sinh(eta2)
    E2 = np.sqrt(px2**2 + py2**2 + pz2**2 + m2**2)

    M2 = (E1 + E2)**2 - (px1 + px2)**2 - (py1 + py2)**2 - (pz1 + pz2)**2
    M = np.sqrt(np.maximum(M2, 0))
    return M


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _flatten(arr):
    """Flatten jagged/awkward arrays to 1D numpy."""
    if ak is not None and isinstance(arr, ak.Array):
        try:
            return ak.to_numpy(ak.flatten(arr))
        except Exception:
            return ak.to_numpy(arr)
    arr = np.asarray(arr)
    return arr.ravel()


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
        prog="cern_analyzer",
        description="CERN Open Data — Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show downloaded catalog
  python cern_analyzer.py catalog ./cern_data

  # Inspect a ROOT file structure
  python cern_analyzer.py inspect ./cern_data/record_700/myfile.root

  # Summary statistics for branches
  python cern_analyzer.py stats ./data/file.root --branches pt eta phi --tree Events

  # Histogram a branch
  python cern_analyzer.py histogram ./data/file.root --branch InvariantMass --bins 200 --log

  # Reconstruct dimuon invariant mass (Z boson peak!)
  python cern_analyzer.py invariant-mass ./data/file.root --particles muon --bins 200

  # Cut-flow analysis
  python cern_analyzer.py cutflow ./data/file.root --cuts "pt>20" "eta<2.4" "nMuon>=2"

  # Compare distributions across two files
  python cern_analyzer.py compare file1.root file2.root --branch pt --normalize

  # Scan all downloaded data
  python cern_analyzer.py scan ./cern_data --ext root --report report.json
        """,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- catalog ---
    sp = sub.add_parser("catalog", help="Show downloaded data catalog")
    sp.add_argument("data_dir", default="./cern_data", nargs="?")

    # --- inspect ---
    sp = sub.add_parser("inspect", help="Inspect file structure")
    sp.add_argument("file")
    sp.add_argument("--depth", type=int, default=3)

    # --- stats ---
    sp = sub.add_parser("stats", help="Branch statistics")
    sp.add_argument("file")
    sp.add_argument("--branches", nargs="+", required=True)
    sp.add_argument("--tree", help="TTree name")
    sp.add_argument("--start", type=int, default=None)
    sp.add_argument("--stop", type=int, default=None)
    sp.add_argument("--save", help="Save stats JSON")

    # --- histogram ---
    sp = sub.add_parser("histogram", help="Generate histogram")
    sp.add_argument("file")
    sp.add_argument("--branch", required=True)
    sp.add_argument("--bins", type=int, default=100)
    sp.add_argument("--xmin", type=float, default=None)
    sp.add_argument("--xmax", type=float, default=None)
    sp.add_argument("--log", action="store_true")
    sp.add_argument("--tree", help="TTree name")
    sp.add_argument("--start", type=int, default=None)
    sp.add_argument("--stop", type=int, default=None)
    sp.add_argument("--output", help="Output image path")
    sp.add_argument("--save-bins", help="Save bin data as JSON")

    # --- invariant-mass ---
    sp = sub.add_parser("invariant-mass", help="Reconstruct invariant mass")
    sp.add_argument("file")
    sp.add_argument("--particles", required=True, help="muon or electron")
    sp.add_argument("--bins", type=int, default=200)
    sp.add_argument("--xmin", type=float, default=None)
    sp.add_argument("--xmax", type=float, default=None)
    sp.add_argument("--log", action="store_true")
    sp.add_argument("--tree", help="TTree name")
    sp.add_argument("--start", type=int, default=None)
    sp.add_argument("--stop", type=int, default=None)
    sp.add_argument("--output", help="Output image path")
    # Manual branch overrides
    sp.add_argument("--pt", help="pt branch name override")
    sp.add_argument("--eta", help="eta branch name override")
    sp.add_argument("--phi", help="phi branch name override")
    sp.add_argument("--mass-branch", help="mass branch name override")

    # --- cutflow ---
    sp = sub.add_parser("cutflow", help="Cut-flow analysis")
    sp.add_argument("file")
    sp.add_argument("--cuts", nargs="+", required=True, help='e.g. "pt>20" "eta<2.4"')
    sp.add_argument("--tree", help="TTree name")
    sp.add_argument("--start", type=int, default=None)
    sp.add_argument("--stop", type=int, default=None)

    # --- compare ---
    sp = sub.add_parser("compare", help="Compare branch across two files")
    sp.add_argument("file1")
    sp.add_argument("file2")
    sp.add_argument("--branch", required=True)
    sp.add_argument("--bins", type=int, default=100)
    sp.add_argument("--normalize", action="store_true")
    sp.add_argument("--log", action="store_true")
    sp.add_argument("--tree", help="TTree name")
    sp.add_argument("--output", help="Output image path")

    # --- scan ---
    sp = sub.add_parser("scan", help="Scan data directory and produce report")
    sp.add_argument("data_dir")
    sp.add_argument("--ext", default="root,csv,json", help="Extensions to scan")
    sp.add_argument("--report", help="Output report JSON path")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    commands = {
        "catalog": cmd_catalog,
        "inspect": cmd_inspect,
        "stats": cmd_stats,
        "histogram": cmd_histogram,
        "invariant-mass": cmd_invariant_mass,
        "cutflow": cmd_cutflow,
        "compare": cmd_compare,
        "scan": cmd_scan,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
