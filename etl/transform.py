# etl/transform.py
import pandas as pd

def enrich_data(df_raw, AOs, date_table, PAXcurrent, PAXdraft, backblast):
    """
    Add in user_names and attributes and AO 
    """
    # Convert to datetime first
    df_raw["date"] = pd.to_datetime(df_raw["date"], format="%b %d, %Y")
    #df_raw["date"] = pd.to_datetime(df_raw["date"], errors="coerce", infer_datetime_format=True)

    # Convert to ISO string (YYYY-MM-DD)
    df_raw["date"] = df_raw["date"].dt.strftime("%Y-%m-%d")

    # Merge with date dimension to bring in 'week'
    df = df_raw.merge(date_table, on="date", how="left")

    # Merge with AO dimension to bring in ao, points, type
    df = df.merge(AOs[["ao_id", "ao", "points", "type"]], on="ao_id", how="left")

    # Merge with PAXdraft to bring in user_name, Team, FNGflag
    df = df.merge(PAXdraft[["user_id", "Team", "FNGflag"]], 
                  on="user_id", how="left")

    # get user_name from PAXcurrent for user_id.
    df = df.merge(PAXcurrent[["user_id", "user_name"]], 
                  on="user_id", how="left", suffixes=("", "_current"))
    
    # get q_user_name from PAXcurrent for user_id.
    df = df.merge(
            PAXcurrent[["user_id", "user_name"]]
                .rename(columns={"user_id": "q_user_id", "user_name": "q_user_name"}),
            on="q_user_id",
            how="left"
        )
            

    # Fill in missing names with constants.  We'll need to create a report with these later.
    # Add new PAX that are not on a team to team None. OR, if FNG, to their team.  Troubleshoot Unknown Names.
    df = df.fillna({"Team": "Unknown Team","user_name": "Unknown Name","q_user_name": "Unknown Name"})

    # Bring in the Backblast string to add notes when desireable
    df = df.merge(backblast[["Date", "AO", "Q", "parsed_backblast", "region"]], 
                  left_on=["date","ao","q_user_name","region"], right_on=["Date","AO","Q","region"], how="left", suffixes=("", "_current")).drop(columns=['Date', 'AO', 'Q'])

    df[["date","ao","q_user_name","region"]]
    backblast[["Date", "AO", "Q","region"]].drop_duplicates().shape[0]
    backblast.shape[0]

    # Select only the columns you want in final df_processed
    df_enriched = df[[
        "date", "week", "ao_id", "q_user_id", "user_id", 
        "ao", "points", "type", "user_name", "Team", "FNGflag", "parsed_backblast"
    ]]

    return df_enriched


def calculate_individual_points(df_enriched: pd.DataFrame) -> pd.DataFrame:
    individual_score_rows = []

    # Loop through each unique user
    for user in df_enriched['user_id'].unique():
        user_df = df_enriched[df_enriched['user_id'] == user].copy()
        user_df = user_df.sort_values(by="date")

        # initialize day and week
        week = None
        day = None
        hardshit=0
        
        # Toss in weekly 6pack bonuses and 6+ points as they are accumulated.
        # Toss in weekly the Around the World bonus (keep track of distinct AOs, when 5, toss in the row
        ATW_list_weekly = []


        for _, row in user_df.iterrows():

            points_to_award = 0
            notes = ""
            
            if row['date']!=day:
                # it's a new day!! reset EC
                day=row['date']
                # set EC_daily and cap EC daily points to 3
                EC_daily = 0

            if row['week']!=week:
                # it's a new week!! reset weekly items
                # For each week, limit QS, QS-Q, 1stFQ, 3rdF, Donation, 2ndF, popup, to 1 occurrence. 
                week=row['week']
                QS_weekly = 0
                QS_Q_weekly = 0
                Q1stF_weekly = 0
                F3rd = 0
                Donation = 0
                F2nd = 0
                popup = 0 
                ATW_Bonus = 0
                ATW_list_weekly = []
                six_pack = 0
                
            if row['type'] == "1stf":
                # he posted at a workout!  lets process this row!
                # add this ao to his AO list
                ATW_list_weekly.append(row['ao'])
                six_pack+=1

                # create the row
                points_to_award = row['points']
                notes = ""

                # check to see if he qualified for Around the world, append another row if he did.
                if len(set(ATW_list_weekly))==5 and ATW_Bonus==0:
                    ATW_Bonus=5
                    new_row_ATW = {
                        "date": row['date'],
                        "week": row['week'],
                        "Team": row['Team'],
                        "user_name": row['user_name'],
                        "ao": None,
                        "type": "Around The World",
                        "points": ATW_Bonus,
                        "notes": tuple(set(ATW_list_weekly))
                        }
                    individual_score_rows.append(new_row_ATW)

                # check to see if he got a 6 pack or 6+
                if six_pack>=6:
                    new_row_6 = {
                        "date": row['date'],
                        "week": row['week'],
                        "Team": row['Team'],
                        "user_name": row['user_name'],
                        "ao": None,
                        "type": "sixpack bonus",
                        "points": 4 if six_pack==6 else 1,
                        "notes": tuple(ATW_list_weekly)
                        }
                    individual_score_rows.append(new_row_6)

                
                # check to see if he was the Q and give him a point.
                # no point if he already Q'd, but still include the row so he knows that I didnt just miss the fact that he Q'd
                if row['q_user_id']==row['user_id'] and row["ao"]!='downrange':
                    new_row_Q = {
                        "date": row['date'],
                        "week": row['week'],
                        "Team": row['Team'],
                        "user_name": row['user_name'],
                        "ao": row['ao'],
                        "type": "workout Q",
                        "points": 1 if Q1stF_weekly==0 else 0,
                        "notes": "Q" if Q1stF_weekly==0 else "you already Q'd a workout this week"
                        }
                    individual_score_rows.append(new_row_Q)
                    Q1stF_weekly=1


            # Feature tested by "EC test (daily cap at 3 points)"
            # Apply EC cap logic
            elif row['type'] == "ec":
                # create the row
                points_to_award = row['points'] if EC_daily+row['points']<=3 else max(0, 3-EC_daily)
                notes = "" if EC_daily+row['points']<=3 else "EC points capped at 3 total per day"
                EC_daily += row['points']

            #Give points for Qsource if he hasnt already gone this week.  Also, for Qing Qsource if he hasnt already Q'd one yet.
            elif row['type'] == "qs":
                # create the row
                points_to_award = row['points'] if QS_weekly==0 else 0
                notes = "QS" if QS_weekly==0 else "Qsource already achieved this week"
                QS_weekly=1
                # check to see if he was the Q and give him a point.
                # no point if he already Q'd, but still include the row so he knows that I didnt just miss the fact that he Q'd
                if row['q_user_id']==row['user_id']:
                    new_row_QSQ = {
                        "date": row['date'],
                        "week": row['week'],
                        "Team": row['Team'],
                        "user_name": row['user_name'],
                        "ao": row['ao'],
                        "type": "QSource Q",
                        "points": 1 if QS_Q_weekly==0 else 0,
                        "notes": "QSQ" if QS_Q_weekly==0 else "you already Q'd a Qsource this week"
                        }
                    individual_score_rows.append(new_row_QSQ)
                    QS_Q_weekly=1

            #need 2ndF, 3rdF, Donation, popup, popup
            elif row['type'] == "2ndf":
                # create the row
                points_to_award = row['points'] if F2nd==0 else 0
                notes = "2ndF" if F2nd==0 else "2nd F already achieved this week"
                F2nd=1
            

            elif row['type'] == "3rdf":
                # create the row
                points_to_award = row['points'] if F3rd==0 else 0
                notes = "3rdF" if F3rd==0 else "3rd F already achieved this week"
                F3rd=1
            
            elif row['type'] == "Donation":
                # create the row
                points_to_award = row['points'] if Donation==0 else 0
                notes = "$$" if Donation==0 else "Donation already achieved this week"
                Donation=1
            
            elif row['type'] == "popup":
                # create the row
                points_to_award = row['points'] if popup==0 else 0
                notes = "popup" if popup==0 else "popup already achieved this week"
                popup=1
            
            elif row['type'] == "hardshit":
                # create the row
                points_to_award = row['points'] if hardshit<2 else 0
                notes = "hardshit" if hardshit<2 else "hardshit already achieved for RG2025"
                hardshit+=1
            
            else:
                continue


            # Build output row
            new_row = {
                "date": row['date'],
                "week": row['week'],
                "Team": row['Team'],
                "user_name": row['user_name'],
                "ao": row['ao'],
                "type": row['type'],
                "points": points_to_award,
                "notes": notes
            }

            individual_score_rows.append(new_row)

            

    # Convert list of dicts to DataFrame
    individual_scores = pd.DataFrame(individual_score_rows)
    return individual_scores


def calculate_team_points(df_enriched: pd.DataFrame, individual_scores: pd.DataFrame, date_table: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate team points.  include individual points.
    Individual points, challenge flags,  s, CSAUPs, FNGs

    date	week	Team	type	points	notes

    types (notes): individual (list of PAX), challenge flag (see below)
        , SL (ao and pax list), CSAUP (pax list), FNG (1st5/posts/VQ - PAX)
    Challenge_flag assumes that a slack blast is entered the day it is won.  The Q is the PAx/Team who won it.
    They keep receiving a point each day (including sunday) until another team wins the flag.
    If multiple teams post a slackblast on the same day, one of them will get the flag.

    """
    

    # Indivdivual contributions ****************************************************************************************************
    team_points_individuals = (individual_scores.groupby(["date", "week", "Team"])
      .agg({
          "points": "sum",
          "user_name": lambda x: list(x.unique())  # or just list(x) if you want duplicates kept
      })
      .reset_index()
      .rename(columns={"user_name": "notes"})
        )
    # Add constant column
    team_points_individuals["type"] = "Individuals points"

    # Reorder columns
    team_points_individuals = team_points_individuals[["date", "week", "Team", "type", "points", "notes"]]



    # calculate challenge flag ***************************************************************************************************
    # first, subset to the challenge flag rows.
    team_flag_scores = []
    df_flag = (
    df_enriched[df_enriched["type"] == "challenge_flag"]
    .sort_values("date", ascending=True)
    .copy()
    )
    # get the dates that you need to go from and to...
    first_date = df_flag["date"].min()
    last_date = df_enriched["date"].max()
    last_team = None
    # get range of dates
    all_dates = pd.date_range(start=first_date, end=last_date).strftime("%Y-%m-%d").tolist()
    # loop through the dates, adding a for every date that awards a point to the team that has the challenge_flag or last had it.
    # if there is more than one team, that 
    for d in all_dates:
        # lookup week from date_table
        week_val = date_table.loc[date_table["date"] == d, "week"].values
        week_val = week_val[0] if len(week_val) > 0 else None
        # get first occurrence of team for that date (if exists)
        rows = df_flag[df_flag["date"] == d]
        if not rows.empty:
            challenge_flag_team = rows.iloc[0]["Team"]
            user_name = rows.iloc[0]["user_name"]
            last_team, last_user = challenge_flag_team, user_name

            # loop through the extra rows, iF there is more than one row for challenge flag on a single day, there shouldnt be, but if there is...
            if len(rows) > 1:
                for _, row in rows.iloc[1:].iterrows():   # skip the first row
                    # build new row
                    new_row_FLAG1 = {
                            "date": d,
                            "week": week_val,
                            "Team": row["Team"],
                            "type": "challenge_flag",
                            "points": 0,
                            "notes": f"No points, {challenge_flag_team} - {user_name} was already awarded for today."
                        }
                    team_flag_scores.append(new_row_FLAG1)


        elif last_team is not None:
            challenge_flag_team, user_name = last_team, last_user
        else:
            continue  # before the first flag is claimed
        
        # create new row for every day after the first flag is claimed.
        new_row_FLAG = {
            "date": d,
            "week": week_val,
            "Team": challenge_flag_team,
            "type": "challenge_flag",
            "points": 1,
            "notes": f"{challenge_flag_team} - {user_name}" if not rows.empty else f"Held from previous claim by {challenge_flag_team} - {user_name}"
        }
        team_flag_scores.append(new_row_FLAG)

        
    team_flag_scores_df = pd.DataFrame(team_flag_scores)
    
    # calculate Santa Locks  ****************************************************************************************************
    df_SantaLocks = (
    df_enriched[df_enriched["ao"].str.startswith("ao", na=False)]
    .sort_values("date", ascending=True)
    .copy()
    )
    # change type
    df_SantaLocks["type"] = "Santa Locks"
    #df_SantaLocks.to_csv("df_SantaLocks.csv", index=False)
    # santa lock aggregation
    df_SantaLocks_summary = (df_SantaLocks.groupby(["date", "week", "Team", "type", "ao"], as_index=False)
            .agg(
                points=("user_name", lambda x: 5 * (len(x) // 5)),
                notes=("user_name", lambda x: ", ".join(x))  # just the names
            )
        )
    # prepend notes with ao
    df_SantaLocks_summary["notes"] = df_SantaLocks_summary["ao"] + "; " + df_SantaLocks_summary["notes"]
    df_SantaLocks_summary = df_SantaLocks_summary[df_SantaLocks_summary["points"] > 0].copy()
    # Reorder columns
    df_SantaLocks_summary = df_SantaLocks_summary[["date", "week", "Team", "type", "points", "notes"]]
    


    # calculate CSAUP (pax list)  ****************************************************************************************************
    df_CSAUP = (
    df_enriched[df_enriched["type"]=="csaup"]
    .sort_values("date", ascending=True)
    .copy()
    )
    
    df_CSAUP_summary = (
    df_CSAUP.groupby(["date", "week", "Team", "type", "q_user_id", "points"], as_index=False)
        .agg(
            notes=("user_name", lambda x: ", ".join(x)),
            name_count=("user_name", "count")
        )
    )

    # Zero out points where fewer than 8 names
    df_CSAUP_summary.loc[df_CSAUP_summary["name_count"] < 8, "points"] = 0

    # Reorder columns and leave out the namer_count column
    df_CSAUP_summary = df_CSAUP_summary[["date", "week", "Team", "type", "points", "notes"]]



    # calculate FNG (1st5/posts/VQ - PAX)  ****************************************************************************************************
    df_FNGs = (
    df_enriched[(df_enriched["FNGflag"]==1) &(df_enriched["type"]=="1stf")]
    .sort_values("date", ascending=True)
    .copy()
    )

    # Track the nth appearance of each user_name
    df_FNGs["appearance_num"] = df_FNGs.groupby("user_name").cumcount() + 1

    # initialize list of lists for output
    FNG_points_lists =[]

    for _, row in df_FNGs.iterrows():
        # Check for 1st appearance
        if row["appearance_num"] == 1:
            FNG_points_lists.append({
                "date": row["date"],
                "week": row["week"],
                "Team": row["Team"],
                "type": "FNG1",
                "points": 3,
                "notes": row["user_name"]
            })

        # Check for 5th appearance
        if row["appearance_num"] == 5:
            FNG_points_lists.append({
                "date": row["date"],
                "week": row["week"],
                "Team": row["Team"],
                "type": "FNG5",
                "points": 5,
                "notes": row["user_name"]
            })

        # Check for q_user_id match
        if row["user_id"] == row["q_user_id"]:
            FNG_points_lists.append({
                "date": row["date"],
                "week": row["week"],
                "Team": row["Team"],
                "type": "FNGQ",
                "points": 7,
                "notes": row["user_name"]
            })

    # Convert to DataFrame and combine
    FNG_points_df = pd.DataFrame(FNG_points_lists)

    
    # Join all dataframes ****************************************************************************************************
    team_scores = (pd.concat([FNG_points_df, df_CSAUP_summary, df_SantaLocks_summary, team_flag_scores_df, team_points_individuals], ignore_index=True)
                 .sort_values("date")
                )
    



    return team_scores
