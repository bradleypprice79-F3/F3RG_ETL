import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306))
}

RAW_DATA = "data/raw_posts/"
RAW_DATA_HOLD = "data/raw_posts/hold"
PROCESSED_DATA = "data/processed/"
REPORTS = "data/reports/"
ARCHIVED_REPORTS= "data/reports/archive_folder"
DIMENSION_DATA = "data/dimensions/"

DAILY_FILE_PATTERN = "*.csv"
DATE_FORMAT = "%Y-%m-%d"

REPORT_TITLE = "Team Scoreboard"

SANTA_LOCK_POINTS = 5
AROUND_THE_WORLD_POINTS = 5


