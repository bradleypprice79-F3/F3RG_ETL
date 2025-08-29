# etl/extract.py
import os
import glob
import pandas as pd
from pathlib import Path

def posts_from_csv_folder(folder_path, file_pattern="*.csv"):
    """
    Reads all CSV files in the given folder matching the file pattern.
    Returns a single concatenated DataFrame.
    """
    all_files = glob.glob(os.path.join(folder_path, file_pattern))
    if not all_files:
        print(f"No files found in {folder_path} with pattern {file_pattern}")
        return pd.DataFrame()  # empty df

    df_list = [pd.read_csv(f) for f in all_files]
    df = pd.concat(df_list, ignore_index=True)
    print(f"Loaded {len(all_files)} files, {len(df)} total rows")
    return df

def extract_dimension_tables(base_path):
    """
    Extracts 4 dimension tables (CSVs) into pandas DataFrames.

    Parameters:
        base_path (str): Path to the folder containing the CSVs.

    Returns:
        tuple: AOs, date_table, PAXcurrent, PAXdraft as pandas DataFrames
    """
    base = Path(base_path)

    AOs = pd.read_csv(base / "AOs.csv")
    date_table = pd.read_csv(base / "date_table.csv")
    PAXcurrent = pd.read_csv(base / "PAXcurrent.csv")
    PAXdraft = pd.read_csv(base / "PAXdraft.csv")

    return AOs, date_table, PAXcurrent, PAXdraft