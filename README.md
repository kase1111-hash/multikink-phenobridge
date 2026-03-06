# Paper V — Experimental Validation Bridge

**Connecting Multi-Kink Generation Framework predictions to CMS Open Data and HEPData measurements.**

This repository accompanies [Paper V: Kaluza–Klein Flavor Physics and Lepton Sector Constraints](https://doi.org/10.XXXX/XXXXX) of the multi-kink generation series. It bridges the framework's structural predictions to publicly available experimental data, establishing quantitative contact between the theoretical parameter space and the current experimental landscape.

The goal is not to test the model directly — the predicted KK scale (M_KK ≳ 5000 TeV) places new physics far beyond current collider reach — but to demonstrate how the framework's structural predictions map onto existing experimental observables and constraints.

---

## Key Results

| Result | Value | Source |
|--------|-------|--------|
| Indirect/direct constraint ratio | **~600×** | Kaon mixing (LR operator) vs CMS Λ limits |
| Chiral fingerprint | \|C'₉/C₉\| ≈ **0.2** | Structural prediction across Pareto frontier |
| P'₅ anomaly (q² ∈ [4.3, 8.68] GeV²) | **~2.5–3σ** tension with SM | CMS 13 TeV, 140 fb⁻¹ (ins2850101) |
| J/ψ peak reconstruction | **3.10 GeV** (PDG: 3.097) | CMS 2012 Open Data, 38.6M μ⁺μ⁻ pairs |
| Z peak reconstruction | **90.90 GeV** (PDG: 91.188) | CMS 2012 Open Data, 6.6M events |
| High-mass tail (200–1000 GeV) | **36,139 events**, no resonance | Consistent with SM Drell-Yan |

---

## Repository Contents

```
paper-v-experimental-bridge/
├── README.md
├── paper_v_bridge_v4.docx          # Bridge document (working draft)
├── tools/
│   ├── cern_downloader.py          # CERN Open Data Portal bulk downloader
│   ├── cern_analyzer.py            # ROOT/CSV analysis (invariant mass, histograms, cutflow)
│   ├── hepdata_downloader.py       # HEPData published measurement downloader
│   └── plot_c9_plane.py            # C₉–C'₉ Wilson coefficient plane generator
├── results/
│   ├── dimuon_spectrum_full.png    # Full spectrum 0.5–120 GeV (37.2M pairs)
│   ├── dimuon_lowmass.png          # ω/ρ zoom 0.5–2.0 GeV
│   ├── dimuon_jpsi.png             # J/ψ + ψ(2S) zoom 2.5–4.5 GeV
│   ├── dimuon_upsilon.png          # Υ(1S,2S,3S) zoom 8.5–11.5 GeV
│   ├── dimuon_zpeak.png            # Z boson zoom 70–110 GeV
│   ├── dimuon_highmass.png         # Z + Drell-Yan tail 60–500 GeV
│   ├── dimuon_veryhighmass.png     # Heavy mediator search region 200–1000 GeV
│   ├── muon_pt.png                 # Muon pT distribution
│   └── c9_c9prime_plane.png        # Wilson coefficient plane
└── data/                           # Not included — see Reproducing below
```

---

## The Three Points of Contact

### 1. Energy Scale Λ: Indirect ≫ Direct (Section 4)

The LR operator dominates both the indirect (Kaon mixing) and direct (LHC dilepton) constraints. Using published CMS limits from the bb→ℓℓ contact interaction search (CMS-EXO-23-010, 2025):

| Chirality | CMS Direct Limit | Paper V Indirect Bound | Ratio |
|-----------|-------------------|------------------------|-------|
| LL (constructive) | Λ > 9.0 TeV | M_KK > 650 TeV (VLL) | 72× |
| **LR (constructive)** | **Λ > 8.3 TeV** | **M_KK > 5000 TeV** | **600×** |

The 600× ratio between indirect and direct sensitivity is the document's central quantitative result. It arises from the chiral enhancement ((m_K/(m_s+m_d))² ≈ 25) and NLO RG running (η_LR ≈ 3.5–4.5) of the LR operator in the Kaon system.

### 2. P'₅ Angular Observable: Chiral Signature (Section 3)

The CMS 13 TeV measurement of B⁰→K*⁰μ⁺μ⁻ angular observables (Phys.Lett.B 2025, 139406) shows a persistent ~2.5–3σ pull from the SM in the q² ∈ [4.3, 8.68] GeV² region. The multi-kink framework predicts that any NP contribution to this channel carries |C'₉/C₉| ≈ 0.2 — a 20% right-handed admixture that distinguishes it from MFV (C'₉ = 0) and typical RS scenarios (|C'₉/C₉| ≪ 0.1).

**The framework predicts the chiral pattern but not at an observable amplitude** at M_KK ≳ 5000 TeV. The P'₅ data defines the experimental benchmark, not a signal.

### 3. Pipeline Validation: Dimuon Spectrum (Section 5, 8)

Analysis of 61.5 million CMS 2012 DoubleMuParked events:

- **38,563,762** opposite-sign muon pairs reconstructed
- All major resonances recovered at PDG-consistent positions (ω/ρ, J/ψ, Υ, Z)
- Smooth Drell-Yan tail to 1 TeV with no resonant excess
- Validates the analysis pipeline end-to-end using only `uproot` and `numpy`

---

## Reproducing the Analysis

### Requirements

```bash
pip install requests tqdm uproot awkward numpy matplotlib pandas pyyaml
```

No CMSSW, Docker, or CERN VM required. Python 3.8+.

### Step 1: Download CMS Open Data

```bash
# CMS 2012 DoubleMuParked — 61.5M events, 2.1 GiB, single ROOT file
python tools/cern_downloader.py direct \
  --url https://opendata.cern.ch/eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012BC_DoubleMuParked_Muons.root \
  --output ./data

# For faster download on high-bandwidth connections:
python tools/cern_downloader.py direct \
  --url https://opendata.cern.ch/eos/opendata/cms/derived-data/AOD2NanoAODOutreachTool/Run2012BC_DoubleMuParked_Muons.root \
  --output ./data --parallel 8
```

**Source:** CERN Open Data Portal, Record 12341. DOI: [10.7483/OPENDATA.CMS.LVG5.QT81](https://doi.org/10.7483/OPENDATA.CMS.LVG5.QT81)

### Step 2: Download HEPData Measurements

```bash
# All Paper V-relevant records (CMS angular analyses, Λ limits, ATLAS dilepton)
python tools/hepdata_downloader.py fetch-all --output ./data/hepdata --format csv
```

Downloads 5 records covering B→K*μμ angular observables, bb→ℓℓ contact interaction limits, and high-mass dilepton searches.

### Step 3: Run the Analysis

```bash
# Full dimuon invariant mass spectrum
python tools/cern_analyzer.py invariant-mass ./data/Run2012BC_DoubleMuParked_Muons.root \
  --particles muon --bins 500 --xmin 0.5 --xmax 120 --log \
  --output results/dimuon_spectrum_full.png

# Resonance zooms
python tools/cern_analyzer.py invariant-mass ./data/Run2012BC_DoubleMuParked_Muons.root \
  --particles muon --bins 200 --xmin 0.5 --xmax 2.0 --output results/dimuon_lowmass.png

python tools/cern_analyzer.py invariant-mass ./data/Run2012BC_DoubleMuParked_Muons.root \
  --particles muon --bins 200 --xmin 2.5 --xmax 4.5 --output results/dimuon_jpsi.png

python tools/cern_analyzer.py invariant-mass ./data/Run2012BC_DoubleMuParked_Muons.root \
  --particles muon --bins 200 --xmin 8.5 --xmax 11.5 --output results/dimuon_upsilon.png

python tools/cern_analyzer.py invariant-mass ./data/Run2012BC_DoubleMuParked_Muons.root \
  --particles muon --bins 200 --xmin 70 --xmax 110 --output results/dimuon_zpeak.png

# High-mass tail
python tools/cern_analyzer.py invariant-mass ./data/Run2012BC_DoubleMuParked_Muons.root \
  --particles muon --bins 200 --xmin 60 --xmax 500 --log --output results/dimuon_highmass.png

python tools/cern_analyzer.py invariant-mass ./data/Run2012BC_DoubleMuParked_Muons.root \
  --particles muon --bins 100 --xmin 200 --xmax 1000 --log --output results/dimuon_veryhighmass.png

# Muon kinematics
python tools/cern_analyzer.py histogram ./data/Run2012BC_DoubleMuParked_Muons.root \
  --branch Muon_pt --bins 200 --xmin 0 --xmax 200 --log --output results/muon_pt.png

# Wilson coefficient plane
python tools/plot_c9_plane.py --output results/c9_c9prime_plane.png
```

### Step 4: Inspect Results

```bash
# File structure
python tools/cern_analyzer.py inspect ./data/Run2012BC_DoubleMuParked_Muons.root

# Branch statistics
python tools/cern_analyzer.py stats ./data/Run2012BC_DoubleMuParked_Muons.root \
  --branches Muon_pt Muon_eta Muon_phi

# HEPData tables
python tools/hepdata_downloader.py inspect --inspire 2850101 --data-dir ./data/hepdata
```

---

## Data Sources

### CERN Open Data Portal

| Record | Dataset | Events | Size | Format |
|--------|---------|--------|------|--------|
| [12341](https://opendata.cern.ch/record/12341) | DoubleMuParked 2012 (NanoAOD, muons only) | 61,540,413 | 2.1 GiB | ROOT |

### HEPData

| INSPIRE | Measurement | Journal | Tables |
|---------|-------------|---------|--------|
| [2850101](https://www.hepdata.net/record/ins2850101) | CMS B⁰→K*⁰μμ angular (13 TeV) | Phys.Lett.B (2025) 139406 | 15 |
| [1385600](https://www.hepdata.net/record/ins1385600) | CMS B⁰→K*⁰μμ angular (8 TeV) | Phys.Lett.B 753 (2016) 424 | 2 |
| [1826544](https://www.hepdata.net/record/ins1826544) | CMS B⁺→K*⁺μμ angular (8 TeV) | JHEP 04 (2021) 124 | 1 |
| [2935112](https://www.hepdata.net/record/ins2935112) | CMS high-mass dilepton + b-jets (13 TeV) | CMS-EXO-23-010 (2025) | 27 |
| [1802523](https://www.hepdata.net/record/ins1802523) | ATLAS high-mass dilepton (13 TeV) | JHEP 11 (2020) 005 | 7 |

---

## Tools

### cern_downloader.py

Search, list, and bulk-download from the CERN Open Data Portal. Supports parallel chunked downloads, checksum verification, resume, and local catalog management.

```bash
python tools/cern_downloader.py search --experiment CMS --type Dataset --limit 10
python tools/cern_downloader.py download --record 12341 --ext root --output ./data
python tools/cern_downloader.py direct --url <URL> --output ./data --parallel 8
```

### cern_analyzer.py

ROOT/CSV analysis tool. Invariant mass reconstruction with opposite-sign pair selection, histogramming, branch statistics, cut-flow analysis, and file comparison. Uses `uproot` for CMSSW-free ROOT file access.

```bash
python tools/cern_analyzer.py inspect <file.root>
python tools/cern_analyzer.py invariant-mass <file.root> --particles muon --bins 500
python tools/cern_analyzer.py stats <file.root> --branches Muon_pt Muon_eta
python tools/cern_analyzer.py histogram <file.root> --branch Muon_pt --bins 200 --log
```

### hepdata_downloader.py

Search and download published measurement tables from HEPData. Includes a curated list of Paper V-relevant records with INSPIRE IDs.

```bash
python tools/hepdata_downloader.py targets              # Show Paper V-relevant records
python tools/hepdata_downloader.py fetch-all --output ./data/hepdata
python tools/hepdata_downloader.py fetch --inspire 2850101 --format csv
```

### plot_c9_plane.py

Generates the C₉–C'₉ Wilson coefficient plane figure showing the multi-kink chiral ratio line against global-fit preferred regions, MFV, and RS anarchic scenarios.

```bash
python tools/plot_c9_plane.py --output results/c9_c9prime_plane.png
```

---

## Multi-Kink Generation Series

| Paper | Title | Status |
|-------|-------|--------|
| I | Exactly Three Normalizable Chiral Zero Modes from a Single Topological Triple-Kink | — |
| II | Yukawa Couplings from Gaussian Wavefunction Overlaps | — |
| III | CKM Matrix from Geometric Flavor Structure | — |
| IV | CP Violation and the Jarlskog Invariant | — |
| **V** | **Kaluza–Klein Flavor Physics and Lepton Sector Constraints** | **This repo** |

---

## Citation

If you use the tools or analysis from this repository:

```bibtex
@misc{branham2026bridge,
  author = {Branham, Kase},
  title = {Experimental Validation Bridge for Paper {V}: {Kaluza--Klein} Flavor Physics},
  year = {2026},
  publisher = {GitHub},
  url = {https://github.com/kase1111-hash/multikink-phenobridge}
}
```

---

## License

Analysis tools: MIT License.

The CMS Open Data used in this analysis is released under [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/). Neither CMS nor CERN endorse any works produced using these data.

HEPData records are subject to their respective licenses as noted on each record page.
