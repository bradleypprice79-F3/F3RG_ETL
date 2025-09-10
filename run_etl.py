
from etl import extract, transform , load, report
import config.config as cfg
from datetime import datetime


def main():
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
    #not_on_a_team_report = transform.no_team_report(df_enriched)

    # Make a timestamp string (e.g. 20250910_1130)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    # 3. Save processed data with timestamp in filename
    load.to_csv(individual_scores, f"{cfg.REPORTS}individual_scores_{timestamp}.csv")
    load.to_csv(team_scores, f"{cfg.REPORTS}team_scores_{timestamp}.csv")

    # Optionally: also write a small manifest file so HTML knows the "latest"
    with open(f"{cfg.REPORTS}latest_files.js", "w") as f:
        f.write(f'const latestFiles = {{\n')
        f.write(f'  individual: "individual_scores_{timestamp}.csv",\n')
        f.write(f'  team: "team_scores_{timestamp}.csv"\n')
        f.write(f'}};\n')

if __name__ == "__main__":
    main()