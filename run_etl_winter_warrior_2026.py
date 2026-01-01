
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
    start_date, end_date = '2025-12-01', '2026-02-28'

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


    # 1. Extract raw post data (CSV for now)
    df_raw = extract.posts_from_csv_folder(cfg.RAW_DATA, cfg.DAILY_FILE_PATTERN)

    AOs, date_table, PAXcurrent, PAXdraft, backblast = extract.extract_dimension_tables(cfg.DIMENSION_DATA)

    # 2. enrich (add user, AO, and date attributes)
    df_enriched = transform.enrich_data(df_raw, AOs, date_table, PAXcurrent, PAXdraft, backblast)
    #df_enriched.to_csv("df_enriched.csv", index=False)





    # 2. Transform (apply scoring rules, individual aggregation)
    individual_scores = transform.calculate_individual_points(df_enriched)
    # 2. Transform (apply scoring rules, team aggregation)
    team_scores = transform.calculate_team_points(df_enriched, individual_scores, date_table)
    # 2. Transform (identify fng's and pax not on a team) (I havent made this function yet, but will later)
    lone_pax_report = transform.get_lone_pax_report(df_enriched)
    # 2. Create the checklist table
    checklist_table = transform.calculate_checklist_table(individual_scores,PAXdraft)
    # 2. Create the individualstandings table
    individualstandings = transform.calculate_individualstandings(individual_scores,team_scores,PAXdraft)

    # 2_5.move any existing data into the archive_folder.  It doesnt hurt anything to stay there, but will make the directory cleaner.
    for file_name in os.listdir(cfg.REPORTS):
        if file_name.endswith(".csv"):
            src_path = os.path.join(cfg.REPORTS, file_name)
            dst_path = os.path.join(cfg.ARCHIVED_REPORTS, file_name)
            print(f"Moving: {file_name} from {src_path} -> {dst_path}")
            shutil.move(src_path, dst_path)
            print(f"Exists in reports? {os.path.exists(src_path)}")
            print(f"Exists in archive? {os.path.exists(dst_path)}")

    # 3. Save processed data with timestamp in filename
    load.to_csv(individual_scores[~individual_scores["Team"].isin(["Unknown Team", "NONE"])], f"{cfg.REPORTS}individual_scores_{timestamp}.csv")
    # don't include unknown team in the team score data.
    load.to_csv(team_scores[~team_scores["Team"].isin(["Unknown Team", "NONE"])], f"{cfg.REPORTS}team_scores_{timestamp}.csv")
    # lone pax report
    load.to_csv(lone_pax_report, f"{cfg.REPORTS}lone_pax_report_{timestamp}.csv")
    # checklist_table
    load.to_csv(checklist_table, f"{cfg.REPORTS}checklist_table_{timestamp}.csv")
    # individualstandings_
    load.to_csv(individualstandings[~individualstandings["Team"].isin(["Unknown Team", "NONE"])], f"{cfg.REPORTS}individualstandings_{timestamp}.csv")

    # Also write a small manifest file so HTML knows the "latest"
    with open(f"{cfg.REPORTS}latest_files.js", "w") as f:
        f.write('const latestFiles = {\n')
        f.write(f'  individual: "individual_scores_{timestamp}.csv",\n')
        f.write(f'  team: "team_scores_{timestamp}.csv",\n')
        f.write(f'  lone_pax: "lone_pax_report_{timestamp}.csv",\n')  
        f.write(f'  aggregated: "checklist_table_{timestamp}.csv",\n') 
        f.write(f'  individualstandings: "individualstandings_{timestamp}.csv",\n') 
        f.write(f'  current_timestamp: "{timestamp_clean}"\n')
        f.write('};\n')

if __name__ == "__main__":
    main()