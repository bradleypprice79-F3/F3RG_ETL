# etl/extract.py
import os
import glob
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine


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



query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'f3crossroads'"  # adjust to your table
df = pd.read_sql(query, engine)

beatdowns
beatdown_info

query = "Select bd_date, ao_id, q_user_id, backblast from f3crossroads.beatdowns"
df = pd.read_sql(query, engine)


aos

query = '''SELECT 
            a.channel_id AS ao_id,
            a.ao,
            999 AS post_count,
            
            CASE 
                WHEN a.ao = 'ao-black-diamond' THEN 4
                WHEN a.ao = '3rd-f-qsource'   THEN 4
                WHEN a.ao = '3rd-f'           THEN 5
                WHEN a.ao = 'rg_ec3'          THEN 3
                WHEN a.ao = 'rg_ec2'          THEN 2
                WHEN a.ao = 'rg_ec1'          THEN 1
                WHEN a.ao = 'rg_challenge_flag' THEN 1
                WHEN a.ao = 'rg_csaup'          THEN 40
                WHEN a.ao = '2nd-f-coffeteria'  THEN 0                
                ELSE 3
            END AS points,
            
            CASE
                WHEN a.ao LIKE 'ao-%%'     THEN '1stf'
                WHEN a.ao LIKE 'downrange%%'     THEN '1stf'
                WHEN a.ao LIKE '2nd-f%%'   THEN '2ndf'
                WHEN a.ao LIKE '3rd-f%%'   THEN '3rdf'
                WHEN a.ao LIKE '%%qsource' THEN 'qs'
                WHEN a.ao LIKE 'rg_ec%%'   THEN 'ec'
                WHEN a.ao LIKE '%%challenge_flag'  THEN 'challenge_flag'
                WHEN a.ao LIKE 'rg_csaup%%'   THEN 'csaup'
                ELSE 'none'
            END AS type
        FROM f3crossroads.aos a'''
df = pd.read_sql(query, engine)



