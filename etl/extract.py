# etl/extract.py
import os
import glob
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
import re
from datetime import date
from dateutil.relativedelta import relativedelta

def clean_backblast(text_string):
    if not isinstance(text_string, str):
        return text_string
    
    # 1. Remove "Backblast! " prefix (case-sensitive)
    text_string = re.sub(r"^Backblast!\s*", "", text)
    
    # 2. Remove all newlines
    text_string = text_string.replace("\n", " ")
    
    # 3. Cut off at "DATE:" (case-insensitive)
    text_string = re.split(r"date:", text_string, flags=re.IGNORECASE)[0].strip()
    
    return text_string

def get_pax_lists(DB_CONFIG):

    # Build connection string
    connection_string = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # create engine
    engine = create_engine(connection_string)

    #define the start and end dates
    end_dt = date.today()
    start_dt = end_dt - relativedelta(months=12)
    start_dt_str = start_dt.strftime('%Y-%m-%d')
    end_dt_str = end_dt.strftime('%Y-%m-%d')

    # Query your data
    # run query to get post data durring the date range.
    raw_post_data_query = text('''SELECT 
        "F3P" as region,
        b.user_id,
        u.user_name,

        -- overall aggregates
        count(*) AS total_posts,
        MAX(b.`date`) AS last_post_date,
        MIN(b.`date`) AS first_post_date,

        -- rolling month buckets (relative to today)
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_last_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 2 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_prior_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 2 MONTH) THEN 1 ELSE 0 END) AS posts_2_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 4 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 3 MONTH) THEN 1 ELSE 0 END) AS posts_3_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 5 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 4 MONTH) THEN 1 ELSE 0 END) AS posts_4_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 5 MONTH) THEN 1 ELSE 0 END) AS posts_5_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 7 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) AS posts_6_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 8 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 7 MONTH) THEN 1 ELSE 0 END) AS posts_7_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 9 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 8 MONTH) THEN 1 ELSE 0 END) AS posts_8_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 10 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 9 MONTH) THEN 1 ELSE 0 END) AS posts_9_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 11 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 10 MONTH) THEN 1 ELSE 0 END) AS posts_10_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 12 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 11 MONTH) THEN 1 ELSE 0 END) AS posts_11_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 13 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 1 ELSE 0 END) AS posts_12_months_ago,

        -- pax_status logic (calculated from the above sums)
        CASE 
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'active'
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'kotter'
            ELSE 'nonkotter'
        END AS pax_status

    FROM f3crossroads.bd_attendance b
    JOIN f3crossroads.aos ao ON ao.channel_id = b.ao_id
    JOIN f3crossroads.users u ON u.user_id = b.user_id
    WHERE ao.ao LIKE 'ao-%'
    AND b.`date` BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()
    GROUP BY b.user_id, u.user_name
                                                                  
    UNION ALL 
    Select "Nprvl" as region,
        b.user_id,
        u.user_name,

        -- overall aggregates
        count(*) AS total_posts,
        MAX(b.`date`) AS last_post_date,
        MIN(b.`date`) AS first_post_date,

        -- rolling month buckets (relative to today)
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_last_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 2 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_prior_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 2 MONTH) THEN 1 ELSE 0 END) AS posts_2_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 4 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 3 MONTH) THEN 1 ELSE 0 END) AS posts_3_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 5 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 4 MONTH) THEN 1 ELSE 0 END) AS posts_4_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 5 MONTH) THEN 1 ELSE 0 END) AS posts_5_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 7 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) AS posts_6_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 8 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 7 MONTH) THEN 1 ELSE 0 END) AS posts_7_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 9 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 8 MONTH) THEN 1 ELSE 0 END) AS posts_8_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 10 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 9 MONTH) THEN 1 ELSE 0 END) AS posts_9_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 11 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 10 MONTH) THEN 1 ELSE 0 END) AS posts_10_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 12 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 11 MONTH) THEN 1 ELSE 0 END) AS posts_11_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 13 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 1 ELSE 0 END) AS posts_12_months_ago,

        -- pax_status logic (calculated from the above sums)
        CASE 
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'active'
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'kotter'
            ELSE 'nonkotter'
        END AS pax_status

    FROM f3naperville.bd_attendance b
    JOIN f3naperville.aos ao ON ao.channel_id = b.ao_id
    JOIN f3naperville.users u ON u.user_id = b.user_id
    WHERE ao.ao LIKE 'ao-%'
    AND b.`date` BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()
    GROUP BY b.user_id, u.user_name
     
    UNION ALL 
    
    Select "CMW" as region,
        b.user_id,
        u.user_name,

        -- overall aggregates
        count(*) AS total_posts,
        MAX(b.`date`) AS last_post_date,
        MIN(b.`date`) AS first_post_date,

        -- rolling month buckets (relative to today)
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_last_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 2 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_prior_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 2 MONTH) THEN 1 ELSE 0 END) AS posts_2_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 4 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 3 MONTH) THEN 1 ELSE 0 END) AS posts_3_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 5 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 4 MONTH) THEN 1 ELSE 0 END) AS posts_4_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 5 MONTH) THEN 1 ELSE 0 END) AS posts_5_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 7 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) AS posts_6_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 8 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 7 MONTH) THEN 1 ELSE 0 END) AS posts_7_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 9 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 8 MONTH) THEN 1 ELSE 0 END) AS posts_8_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 10 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 9 MONTH) THEN 1 ELSE 0 END) AS posts_9_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 11 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 10 MONTH) THEN 1 ELSE 0 END) AS posts_10_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 12 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 11 MONTH) THEN 1 ELSE 0 END) AS posts_11_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 13 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 1 ELSE 0 END) AS posts_12_months_ago,

        -- pax_status logic (calculated from the above sums)
        CASE 
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'active'
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'kotter'
            ELSE 'nonkotter'
        END AS pax_status

    FROM  `f3cha-min-wood`.bd_attendance b
    JOIN  `f3cha-min-wood`.aos ao ON ao.channel_id = b.ao_id
    JOIN  `f3cha-min-wood`.users u ON u.user_id = b.user_id
    WHERE ao.ao LIKE 'ao-%'
    AND b.`date` BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()
    GROUP BY b.user_id, u.user_name
                
                               




    UNION ALL
    
    SELECT 
        "Outlands" as region,
        b.user_id,
        u.user_name,

        -- overall aggregates
        count(*) AS total_posts,
        MAX(b.`date`) AS last_post_date,
        MIN(b.`date`) AS first_post_date,

        -- rolling month buckets (relative to today)
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_last_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 2 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS posts_prior_month,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 2 MONTH) THEN 1 ELSE 0 END) AS posts_2_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 4 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 3 MONTH) THEN 1 ELSE 0 END) AS posts_3_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 5 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 4 MONTH) THEN 1 ELSE 0 END) AS posts_4_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 5 MONTH) THEN 1 ELSE 0 END) AS posts_5_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 7 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) AS posts_6_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 8 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 7 MONTH) THEN 1 ELSE 0 END) AS posts_7_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 9 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 8 MONTH) THEN 1 ELSE 0 END) AS posts_8_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 10 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 9 MONTH) THEN 1 ELSE 0 END) AS posts_9_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 11 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 10 MONTH) THEN 1 ELSE 0 END) AS posts_10_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 12 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 11 MONTH) THEN 1 ELSE 0 END) AS posts_11_months_ago,
        SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 13 MONTH) 
                AND b.`date` <= DATE_SUB(CURDATE(), INTERVAL 12 MONTH) THEN 1 ELSE 0 END) AS posts_12_months_ago,

        -- pax_status logic (calculated from the above sums)
        CASE 
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'active'
            WHEN SUM(CASE WHEN b.`date` > DATE_SUB(CURDATE(), INTERVAL 6 MONTH) THEN 1 ELSE 0 END) > 0 THEN 'kotter'
            ELSE 'nonkotter'
        END AS pax_status

    FROM f3outlands.bd_attendance b
    JOIN f3outlands.aos ao ON ao.channel_id = b.ao_id
    JOIN f3outlands.users u ON u.user_id = b.user_id
    WHERE ao.ao LIKE 'ao-%'
    AND b.`date` BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()
    GROUP BY b.user_id, u.user_name
    
              '''
    )
    post_df = pd.read_sql(raw_post_data_query, engine, params={"start_date": start_dt_str, "end_date": end_dt_str})
    return(post_df)

def get_raw_posts(DB_CONFIG, start_date, end_date):

    # Build connection string
    connection_string = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # create engine
    engine = create_engine(connection_string)

    # Query your data
    # run query to get post data durring the date range.
    raw_post_data_query = text('''SELECT 
        `date`,
        'f3crossroads' AS region,
        ao_id,
        q_user_id,
        user_id,
        1 AS `Current Post Count`
    FROM f3crossroads.bd_attendance
        WHERE `date` >= :start_date
          AND `date` <= :end_date '''
    )
    post_df = pd.read_sql(raw_post_data_query, engine, params={"start_date": start_date, "end_date": end_date})
    return(post_df)

def get_raw_dimension_data(DB_CONFIG, start_date, end_date):

    # Build connection string
    connection_string = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # create engine
    engine = create_engine(connection_string)

    # Query your data
    AOs_raw_query = text('''SELECT 
            a.channel_id AS ao_id,
            a.ao,
            999 AS post_count,
            
            CASE 
                WHEN a.ao = 'ao-black-diamond' THEN 4
                WHEN a.ao = '3rd-f-qsource'   THEN 5
                WHEN a.ao = '3rd-f'           THEN 5
                WHEN a.ao = 'rg_ec3'          THEN 3
                WHEN a.ao = 'rg_hardshit'     THEN 10
                WHEN a.ao = 'rg_ec2'          THEN 2
                WHEN a.ao = 'rg_ec1'          THEN 1
                WHEN a.ao = 'rg_challenge_flag' THEN 1
                WHEN a.ao = 'rg_csaup'          THEN 80
                WHEN a.ao = '2nd-f-coffeteria'  THEN 0    
                WHEN a.ao = '2nd-f'  THEN 5    
                ELSE 3
            END AS points,
            
            CASE
                WHEN a.ao LIKE 'ao-%%'     THEN '1stf'
                WHEN a.ao LIKE 'downrange%%'     THEN '1stf'
                WHEN a.ao LIKE '2nd-f%%'   THEN '2ndf'
                WHEN a.ao LIKE '%%qsource' THEN 'qs'
                WHEN a.ao LIKE '3rd-f%%'   THEN '3rdf'
                WHEN a.ao LIKE 'rg_ec%%'   THEN 'ec'
                WHEN a.ao LIKE 'rg_hard%%'   THEN 'hardsh!t'
                WHEN a.ao LIKE '%%challenge_flag'  THEN 'challenge_flag'
                WHEN a.ao LIKE 'rg_csaup%%'   THEN 'csaup'
                WHEN a.ao LIKE 'rg_3rdf_donation%%'   THEN 'Donation'
                WHEN a.ao LIKE 'rg_popup%%'   THEN 'popup'
                ELSE 'none'
            END AS type
        FROM f3crossroads.aos a '''
    )
    AOs_raw = pd.read_sql(AOs_raw_query, engine)

    PAXcurrent_raw_query = text('''Select user_id, user_name from f3crossroads.users ''')
    PAXcurrent_raw = pd.read_sql(PAXcurrent_raw_query, engine)

    backblast_raw_query = text('''Select bd_date, ao_id, q_user_id, backblast from f3crossroads.beatdowns ''' )
    backblast_raw = pd.read_sql(backblast_raw_query, engine)

    ## create date list..
    # Create date range
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    # Initialize week counter
    weeks = []
    week_num = 0
    for d in dates:
        if d.weekday() == 6:  # Sunday (0=Monday, 6=Sunday)
            week_num += 1
        weeks.append(week_num)

    # Build dataframe
    df_dates = pd.DataFrame({"date": dates, "week": weeks})



    return AOs_raw, PAXcurrent_raw, backblast_raw, df_dates



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


    return AOs, date_table, PAXcurrent, PAXdraft, backblast
