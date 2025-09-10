# etl/extract.py
import os
import glob
import pandas as pd
from pathlib import Path
import mysql.connector

def get_raw_posts():


    # Load credentials from environment
    host = os.environ.get("DB_HOST")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    database = os.environ.get("DB_NAME")
    port = int(os.environ.get("DB_PORT", 3306))
    ssl_ca = os.environ.get("DB_SSL_CA")

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        ssl_ca=ssl_ca,
        ssl_disabled=False,
        unix_socket=None  # ensures TCP is used
    )

    # Query your data
    query = "SELECT * FROM your_table_name;"  # adjust to your table
    df = pd.read_sql(query, conn)

    # Close connection
    conn.close()

    # Save CSV to raw_data folder
    df.to_csv("data/raw_data/my_table.csv", index=False)
    print("Saved CSV to data/raw_data/my_table.csv")

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
    backblast = pd.read_csv(base / "backblast.csv")

    # remove \n newline characters from backblast.
    backblast["parsed_backblast"] = backblast["parsed_backblast"].str.replace("\n", " ", regex=False)
    # Drop "DATE: ..." and everything after
    backblast["parsed_backblast"] = backblast["parsed_backblast"].str.replace(r"DATE:.*", "", regex=True).str.strip()


    return AOs, date_table, PAXcurrent, PAXdraft, backblast