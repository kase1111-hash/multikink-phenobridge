#!/usr/bin/env python3
"""
C9 vs C'9 Wilson Coefficient Plane
====================================
Generates the chiral structure plot for Paper V bridge document.

Shows:
  - SM point (C9=0, C'9=0 in NP convention)
  - Global-fit preferred region (C9 ~ -0.5 to -1.5)
  - Multi-kink predicted ratio line: C'9 = 0.2 * C9
  - MFV line: C'9 = 0 (no tree-level right-handed FCNCs)
  - Current experimental sensitivity region

Usage:
    python plot_c9_plane.py [--output c9_c9prime_plane.png]

Requirements:
    pip install matplotlib numpy
"""

import argparse
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse, FancyArrowPatch
from matplotlib.lines import Line2D

def main():
    parser = argparse.ArgumentParser(description="C9 vs C'9 Wilson coefficient plane")
    parser.add_argument("--output", default="c9_c9prime_plane.png")
    args = parser.parse_args()

    fig, ax = plt.subplots(figsize=(9, 7))

    # ─── Axis setup ───
    ax.set_xlim(-2.5, 1.0)
    ax.set_ylim(-0.8, 0.8)
    ax.set_xlabel(r"$C_9^{\mathrm{NP}}$", fontsize=16)
    ax.set_ylabel(r"$C_9^{\prime\,\mathrm{NP}}$", fontsize=16)
    ax.set_title(r"Wilson Coefficient Plane: $C_9^{\mathrm{NP}}$ vs $C_9^{\prime\,\mathrm{NP}}$",
                 fontsize=15, fontweight="bold", pad=15)
    ax.axhline(0, color="gray", linewidth=0.5, zorder=0)
    ax.axvline(0, color="gray", linewidth=0.5, zorder=0)
    ax.grid(True, alpha=0.2)

    # ─── SM point ───
    ax.plot(0, 0, "k*", markersize=18, zorder=10, label="SM point")
    ax.annotate("SM", (0, 0), (0.15, 0.08), fontsize=12, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color="black", lw=1.2))

    # ─── Global fit preferred region (schematic) ───
    # Based on Altmannshofer & Straub; Descotes-Genon, Matias, Virto; Hurth, Mahmoudi
    # Typical 1σ: C9 ~ [-1.3, -0.7], C'9 ~ [-0.3, 0.3]
    # 2σ: C9 ~ [-1.8, -0.3], C'9 ~ [-0.5, 0.5]
    ell_1sigma = Ellipse((-1.0, 0.0), width=0.7, height=0.5, angle=-5,
                          facecolor="#4A90D9", alpha=0.25, edgecolor="#2E5FA1",
                          linewidth=2, zorder=5, label=r"Global fit $1\sigma$ (schematic)")
    ell_2sigma = Ellipse((-1.0, 0.0), width=1.4, height=0.9, angle=-5,
                          facecolor="#4A90D9", alpha=0.10, edgecolor="#2E5FA1",
                          linewidth=1.5, linestyle="--", zorder=4,
                          label=r"Global fit $2\sigma$ (schematic)")
    ax.add_patch(ell_2sigma)
    ax.add_patch(ell_1sigma)

    # ─── Multi-kink prediction line: C'9 = 0.2 * C9 ───
    c9_line = np.linspace(-2.5, 1.0, 100)
    c9p_line = 0.2 * c9_line
    ax.plot(c9_line, c9p_line, color="#CC0000", linewidth=2.5, linestyle="-",
            zorder=8, label=r"Multi-kink: $|C_9^{\prime}/C_9| = 0.2$")

    # Show the ±0.2 band to indicate it's approximate
    ax.fill_between(c9_line, 0.15 * c9_line, 0.25 * c9_line,
                     color="#CC0000", alpha=0.08, zorder=3)

    # ─── MFV line: C'9 = 0 ───
    ax.axhline(0, color="#228B22", linewidth=2, linestyle=":", zorder=7,
               label=r"MFV: $C_9^{\prime} = 0$")

    # ─── RS anarchic (schematic range) ───
    # In RS, |C'9/C9| depends on bR, sR localization; typically small but model-dependent
    c9p_rs_low = -0.05 * c9_line
    c9p_rs_high = 0.05 * c9_line
    ax.fill_between(c9_line, c9p_rs_low, c9p_rs_high,
                     color="#228B22", alpha=0.08, zorder=2,
                     label=r"RS anarchic: $|C_9^{\prime}/C_9| \ll 0.1$ (typical)")

    # ─── Annotation: where the multi-kink line passes through the fit region ───
    # At C9 = -1.0 (center of global fit), C'9 = -0.2
    ax.plot(-1.0, -0.2, "o", color="#CC0000", markersize=10, zorder=11,
            markeredgecolor="white", markeredgewidth=1.5)
    ax.annotate(r"$C_9^{\mathrm{NP}} = -1.0$" + "\n" + r"$C_9^{\prime} = -0.2$",
                (-1.0, -0.2), (-1.8, -0.55), fontsize=10,
                arrowprops=dict(arrowstyle="->", color="#CC0000", lw=1.2),
                color="#CC0000", bbox=dict(boxstyle="round,pad=0.3",
                                           facecolor="white", edgecolor="#CC0000", alpha=0.9))

    # ─── Note about multi-kink amplitude ───
    ax.text(-2.4, 0.72,
            "Note: Multi-kink framework predicts\n"
            r"$|C_9^{\mathrm{NP}}| \ll 10^{-4}$ at $M_{KK} \gtrsim 5000$ TeV" + "\n"
            "Line shows chiral ratio, not amplitude.",
            fontsize=9, color="#666666", fontstyle="italic",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="#FFF8F0",
                      edgecolor="#CC6600", alpha=0.9),
            verticalalignment="top")

    # ─── Legend ───
    ax.legend(loc="lower left", fontsize=10, framealpha=0.95,
              edgecolor="#CCCCCC")

    # ─── References note ───
    ax.text(0.98, 0.02,
            "Global fit contours schematic;\n"
            "see Altmannshofer & Straub (2017),\n"
            "Descotes-Genon et al. (2016),\n"
            "Hurth, Mahmoudi & Neshatpour (2020)",
            transform=ax.transAxes, fontsize=7.5, color="#999999",
            ha="right", va="bottom")

    fig.tight_layout()
    fig.savefig(args.output, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Plot saved → {args.output}")


if __name__ == "__main__":
    main()
