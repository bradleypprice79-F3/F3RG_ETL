
from etl import extract, transform, load, report
import config.config as cfg


def main():
    # 1. Extract raw post data (CSV for now)
    df_raw = extract.posts_from_csv_folder(cfg.RAW_DATA, cfg.DAILY_FILE_PATTERN)

    AOs, date_table, PAXcurrent, PAXdraft = extract.extract_dimension_tables(cfg.DIMENSION_DATA)

    # 2. enrich (add user, AO, and date attributes)
    df_enriched = transform.enrich_data(df_raw, AOs, date_table, PAXcurrent, PAXdraft)
    df_enriched.to_csv("df_enriched.csv", index=False)

    # 2. Transform (apply scoring rules, individual aggregation)
    individual_scores = transform.calculate_individual_points(df_enriched)
    # 2. Transform (apply scoring rules, team aggregation)
    team_scores = transform.calculate_team_points(df_enriched, individual_scores, date_table)
    # 2. Transform (identify fng's and pax not on a team)
    not_on_a_team_report = transform.no_team_report(df_enriched)



    # 3. Save processed data
    individual_scores.to_csv("output_file.csv", index=False)
    load.to_csv(individual_scores, cfg.PROCESSED_DATA + "processed.csv")

    # 4. Generate report HTML
    html = report.generate(team_scores, title=cfg.REPORT_TITLE)
    load.to_html(html, cfg.REPORTS + "index.html")

if __name__ == "__main__":
    main()