# -*- coding: utf-8 -*-
"""
Created on Wed Oct 22 08:57:59 2025

@author: enric
"""

import pandas as pd
import os
import glob
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable


def duration_to_minutes(s: pd.Series) -> pd.Series:
    """
    Normalize duration column.
    It handles:
      - numbers (es. 2880)
      - timedelta string (es. '0 days 00:04:00', '2 days 00:01:00')
    Returns Int64 (nullable).
    """
    # Case 1: already numeric
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").round().astype("Int64")

    # Case 2: strig
    s_str = s.astype(str).str.strip()

    td = pd.to_timedelta(s_str, errors="coerce")
    if td.notna().any():
        mins = (td / pd.Timedelta(minutes=1)).round()
        return mins.astype("Int64")

    # Case 3: fallback
    mins_num = pd.to_numeric(s_str, errors="coerce")
    return mins_num.round().astype("Int64")

def main():
    
    BASE_DIR = Path("C:/Users/enric/Desktop/Finding Gaps/gaps.py").resolve().parent
    DATA_DIR = BASE_DIR / "data"

    folders = [
        DATA_DIR / "dukascopy" / "cleaned",
        DATA_DIR / "ibkr" / "cleaned",
        DATA_DIR / "dukascopy" / "live_jforex" / "cleaned",
        DATA_DIR / "dukascopy" / "live_jforex_2" / "cleaned",
        DATA_DIR / "ibkr" / "live_ibkr" / "cleaned",
        DATA_DIR / "ibkr" / "live_ibkr_2" / "cleaned",
    ]
    
    objects = [
        {"obj": "_invalid_blocks", "OBJ": "Invalid blocks", "s":"start", "e":"end","d":"duration"},
        {"obj": "_short_gaps",   "OBJ": "Short term", "s":"gap_start", "e":"gap_end", "d":"gap_duration_min"}, 
        {"obj": "_long_gaps", "OBJ": "Long Term", "s":"gap_start", "e":"gap_end", "d":"gap_duration_min"},
    ]
    
    for folder in folders:
        for o in objects:
    
            obj = o["obj"]
            OBJ = o["OBJ"]
            s = o["s"]
            e = o["e"]
            d = o["d"]
            
            pattern = str(folder / f"*{obj}.parquet")
            files = glob.glob(pattern)
            print(f"\nFound {len(files)} {OBJ} files in folder {folder}.\n")
    
            for file_path in files:
                file_name = Path(file_path).stem.replace(f"{obj}", "")
                pair_name = "_".join(file_name.split("_")[:3])
                
                folder_path = Path(f"{folder}/{OBJ}_figures")
    
            
                save_path1 = folder_path / f"{pair_name}_timeline.png"
                save_path2 = folder_path / f"{pair_name}_duration_hist.png"
                save_path3 = folder_path / f"{pair_name}_heatmap.png"
            
                print(f"Loading {file_name}...")
                
                #Read and normalize columns
                df = pd.read_parquet(file_path)
                #Always create output folder 
                os.makedirs(folder_path, exist_ok=True)
                    
                #We keep the columns: start, end, is_invalid, n_rows, duration
                df["start"] = pd.to_datetime(df[f"{s}"], utc=True)
                df["end"]   = pd.to_datetime(df[f"{e}"],   utc=True)
            
                # 'duration' in CSV is a string of the type: "0 days 00:05:00" → Timedelta
                df["duration_min"] = duration_to_minutes(df[d])
    
    
                #Gap scheme
                gaps_df = pd.DataFrame({
                    "start": df["start"],
                    "end": df["end"],
                    "duration_min": df["duration_min"]
                }).sort_values("start").reset_index(drop=True)
            
                if gaps_df.empty:
                    print(f"No {OBJ} here.")
                    continue
            
                #Heatmap attributes 
                gaps_df["weekday"] = gaps_df["start"].dt.weekday
                gaps_df["hour"]    = gaps_df["start"].dt.hour
            
                # =========================
                # 1) TIMELINE (tic for short gaps, bars for long gaps)
                # =========================
                SHORT_THR_MIN = 5
                short = gaps_df[gaps_df["duration_min"] < SHORT_THR_MIN]
                long  = gaps_df[gaps_df["duration_min"] >= SHORT_THR_MIN]
                fig, ax = plt.subplots(figsize=(12, 3))
                if not short.empty:
                    ax.vlines(short["start"], -0.18, +0.18, linewidth=0.8, alpha=0.8)
            
                if not long.empty:
                    starts = mdates.date2num(long["start"])
                    ends   = mdates.date2num(long["end"])
                    widths = ends - starts
            
                    dur = long["duration_min"].values
                    norm = Normalize(vmin=np.percentile(dur, 5), vmax=np.percentile(dur, 95))
                    cmap = plt.cm.viridis
                    colors = cmap(norm(dur))
            
                    for (start, width, color) in zip(starts, widths, colors):
                        ax.broken_barh([(start, width)], (-0.30, 0.6),
                                       facecolors=color, edgecolors="none", alpha=0.9)
            
                    sm = ScalarMappable(norm=norm, cmap=cmap)
                    sm.set_array([])
                    cbar = plt.colorbar(sm, ax=ax, pad=0.01)
                    cbar.set_label(f"{OBJ} duration (min)")
            
                ax.set_yticks([0]); ax.set_yticklabels([f"{OBJ}"])
                ax.set_ylim(-0.6, 0.6)
                ax.xaxis.set_major_locator(mdates.AutoDateLocator())
                ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
                ax.set_title(f"{OBJ} gap timeline — {pair_name}")
                ax.set_xlabel("Time (UTC)")
                plt.savefig(save_path1, dpi=300, bbox_inches='tight')
                plt.tight_layout()
                plt.close()
            
                # ------------------------------------------------------------
                # 2) HISTOGRAM OF GAP DURATIONS
                # ------------------------------------------------------------
            
                dur = gaps_df["duration_min"].astype(int).values
                vc = pd.Series(dur).value_counts().sort_index()
            
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.bar(vc.index, vc.values, width=1)
                ax.set_ylim(0, vc.values.max() * 1.1)
                ax.set_xlim(0, np.percentile(dur, 99))
                ax.set_title(f"{OBJ} Duration Distribution — {pair_name}")
                ax.set_xlabel("Duration (minutes)")
                ax.set_ylabel("Count")
            
                textstr = f"Total blocks: {len(gaps_df)}\nMedian: {int(np.median(dur))} min\nMax: {int(np.max(dur))} min"
                ax.text(0.98, 0.95, textstr, transform=ax.transAxes, ha='right', va='top',
                        bbox=dict(boxstyle="round", fc="white", alpha=0.8))
                plt.savefig(save_path2, dpi=300, bbox_inches='tight')
                plt.tight_layout()
                plt.close()
            
                # ------------------------------------------------------------
                # 3) HEATMAP weekday x hour
                # ------------------------------------------------------------
            
                heat = (
                    gaps_df.groupby(["weekday","hour"])
                           .size()
                           .reindex(pd.MultiIndex.from_product([range(7), range(24)],
                                                               names=["weekday","hour"]),
                                    fill_value=0)
                           .unstack("hour")
                )
            
                day_labels = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
                fig, ax = plt.subplots(figsize=(10, 3.8))
                im = ax.imshow(heat.values, aspect="auto", origin="upper")
                ax.set_yticks(range(7)); ax.set_yticklabels(day_labels)
                ax.set_xticks(range(0,24,2)); ax.set_xticklabels([f"{h:02d}:00" for h in range(0,24,2)])
                ax.set_title(f"Hourly/Weekday {OBJ} Frequency — {pair_name}")
                ax.set_xlabel("Hour (UTC)"); ax.set_ylabel("Weekday")
                cbar = plt.colorbar(im, ax=ax); cbar.set_label("Count")
                plt.savefig(save_path3, dpi=300, bbox_inches='tight')
                plt.tight_layout()
                plt.close()
                
        print(f"\nAll analyses were performed for the folder {folder}\n")

    

if __name__ == "__main__":
    main()
        
        
