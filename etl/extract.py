# etl/extract.py
import os
import glob
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

def clean_backblast(text):
    if not isinstance(text, str):
        return text
    
    # 1. Remove "Backblast! " prefix (case-sensitive)
    text = re.sub(r"^Backblast!\s*", "", text)
    
    # 2. Remove all newlines
    text = text.replace("\n", " ")
    
    # 3. Cut off at "DATE:" (case-insensitive)
    text = re.split(r"date:", text, flags=re.IGNORECASE)[0].strip()
    
    return text

def get_raw_posts():


    # Load credentials from environment
    host = os.environ.get("DB_HOST")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    database = os.environ.get("DB_NAME")
    port = int(os.environ.get("DB_PORT", 3306))
    ssl_ca = os.environ.get("DB_SSL_CA")

# build the connection string
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

# create engine
engine = create_engine(connection_string)

# Query your data
# run query
raw_post_data_query = '''SELECT 
    `date`,
    'f3crossroads' AS region,
    ao_id,
    q_user_id,
    user_id,
    1 AS `Current Post Count`
FROM f3crossroads.bd_attendance
WHERE `date`>='2025-07-11' '''
post_df = pd.read_sql(raw_post_data_query, engine)
print(post_df.head())

query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'f3crossroads'"  # adjust to your table
df = pd.read_sql(query, conn)
query = "SELECT * FROM f3crossroads.bd_attendance limit 10"  # adjust to your table
df = pd.read_sql(query, conn)
    backblast

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