# FX High-Frequency Data Cleaning and Missing Data Diagnostics

This repository contains a complete and reproducible pipeline for cleaning high-frequency foreign exchange (FX) OHLC time series and for diagnosing missing data patterns.  
The project was developed in the context of an academic thesis focused on data integrity, microstructure issues, and provider-specific artifacts in FX minute-level datasets.

The workflow is designed to:
- clean raw FX data obtained from different providers,
- identify missing observations and structurally invalid price blocks,
- generate diagnostic figures that highlight temporal patterns of data gaps.

---

## Data Sources

The analysis focuses on FX data obtained from two major providers:

- **Dukascopy**: provides free historical tick-level bid/ask data. While extremely valuable for research, its historical minute-bar feed does not guarantee continuity and often exhibits short micro-gaps, irregular tick density, and invalid price blocks.
- **Interactive Brokers (IBKR)**: provides a more stable and execution-oriented FX feed. Minute bars are highly regular during trading hours, with gaps concentrated around deterministic rollover windows.

Both historical datasets and live-recorded data were analyzed to validate the observed behaviors.

---

## Requirements

- Python **3.10** or later

### Main dependencies
- `pandas`
- `numpy`
- `matplotlib`

### Optional dependency
Required only if cleaned datasets are stored in **Parquet** format instead of CSV:
- `pyarrow`

---

## Usage

1. Place raw datasets in the appropriate `data/**/raw/` folders.

2. Run the cleaning pipeline:

    python data_cleaning.py

3. Generate diagnostic figures:

    python data_gaps.py

---

## Repository Structure

The repository expects the following folder structure:

```text
your-repo/
├─ data/
│  ├─ dukascopy/
│  │  ├─ raw/
│  │  ├─ cleaned/
│  │  ├─ live_jforex/
│  │  │  ├─ raw/
│  │  │  └─ cleaned/
│  │  └─ live_jforex_2/
│  │     ├─ raw/
│  │     └─ cleaned/
│  └─ ibkr/
│     ├─ raw/
│     ├─ cleaned/
│     ├─ live_ibkr/
│     │  ├─ raw/
│     │  └─ cleaned/
│     └─ live_ibkr_2/
│        ├─ raw/
│        └─ cleaned/
│
├─ data_cleaning.py
├─ data_gaps.py
├─ gaps.py
└─ README.md



All paths are built relative to the location of "gaps.py" using:

```python
BASE_DIR = Path("C:/Users/enric/gaps.py").resolve().parent
DATA_DIR = BASE_DIR / "data"