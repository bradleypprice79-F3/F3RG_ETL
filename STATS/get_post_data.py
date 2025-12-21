from etl import extract, transform , load ###, report
import config.config as cfg
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import shutil


def main():
    #GET DATA FOR pax lists
    #pax_list_data = extract.get_pax_lists(cfg.DB_CONFIG)
    #load.to_csv(pax_list_data, f"{cfg.DIMENSION_DATA}PAX_LIST.csv")


    # Make a timestamp string (e.g. 20250910_1130)
    # Use America/Chicago for Central Time (handles CST/CDT automatically)
    timestamp = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y%m%d_%H%M")
    timestamp_clean = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d %H:%M")

    #define the start and end dates
    start_date, end_date = '2025-11-01', '2025-12-13'

    # 0. Get data from MySQL and save raw data.
    post_raw = extract.get_raw_posts(cfg.DB_CONFIG, start_date, end_date)
    AOs_raw, PAXcurrent_raw, backblast_raw, df_dates = extract.get_raw_dimension_data(cfg.DB_CONFIG, start_date, end_date)
    

    # move old raw post data from raw_posts to hold.
    for file_name in os.listdir(cfg.RAW_DATA):
        if file_name.startswith("raw_posts_20"):
            src_path = os.path.join(cfg.RAW_DATA, file_name)
            dst_path = os.path.join(cfg.RAW_DATA_HOLD, file_name)
            print(f"Moving: {file_name} from {src_path} -> {dst_path}")
            shutil.move(src_path, dst_path)
            print(f"Exists in reports? {os.path.exists(src_path)}")
            print(f"Exists in archive? {os.path.exists(dst_path)}")
    
    # Save CSV to raw_data folder
    load.to_csv(post_raw, f"{cfg.RAW_DATA}raw_posts_{timestamp}.csv")
    # overwrite dimension data with updates from MySQL
    load.to_csv(AOs_raw, f"{cfg.DIMENSION_DATA}AOs.csv")
    load.to_csv(PAXcurrent_raw, f"{cfg.DIMENSION_DATA}PAXcurrent.csv")
    load.to_csv(backblast_raw, f"{cfg.DIMENSION_DATA}backblast.csv")
    load.to_csv(df_dates, f"{cfg.DIMENSION_DATA}date_table.csv")

