import pandas as pd

# Build synthetic test dataset
rows = []
output_individual = []

# Helper to add a row
def add_row(date, region, ao_id, q_user_id, user_id, post_count, notes, lines):
    rows.append({
        "date": date,
        "region": region,
        "ao_id": ao_id,
        "q_user_id": q_user_id,
        "user_id": user_id,
        "Current Post Count": post_count,
        "notes": notes,
        "lines": lines
    })

# Helper to add an output row
#date,week,Team,user_name,ao,type,points,notes
#2024-11-02,0.0,Donner,Bedpan,ao-the-ridge,1stf,3.0,ao-the-ridge
#2024-11-02,0.0,Donner,Bedpan,ao-the-colosseum,1stf,3.0,ao-the-colosseum
def out_individual_row(date,week,Team,user_name,ao,type,points,notes):
    output_individual.append({
        "date": date,
        "week": week,
        "Team": Team,
        "user_name": user_name,
        "ao": ao,
        "type": type,
        "points": points,
        "notes": notes
    })

# We'll use user_ids from PAXdraft plus some FNGs
# Existing users (from your PAXdraft sample)
users = {
    "meta": "U03SX99V3PU",
    "chum": "U03765UPB8C",
    "cutler": "U05LL7ESE79",
    "villa": "U017QPPJVJ5",
    "depends": "U051330RY2J",
    "dauber": "U02A96W697X",
    "foreclosure": "U03TX17L7AQ",
    "boomer": "U06GZ8ECZC5",
}
# Add a new FNG
users["fng1"] = "U0FNG000001"


# Existing AOS for testing
aos = {
    "rg_ec2": "C09BCTEBDE2",
    "rg_ec1": "C09CLKCEU8N",
    "rg_csaup": "C09CF33SXD1",
    "rg_hardshit": "C09CHEV0B6Z",
    "rg_3rdf_donation": "C09CHEY6NRK",
    "rg_popup": "C09CKB60890",
    "rg_challenge_flag": "C09DDA04MSL",
    "ao-ravens-nest": "C06FH4FCF5Y",
    "ao-black-diamond": "C02DNLUQN9Y",
    "ao-the-ridge": "C02B4U0M71N",
    "ao-the-grove": "C04PLDCTSJV",
    "ao-the-olympiad": "C02A0HGC4PR",
    "ao-the-colosseum": "C02AC8PS1T7",
    "ao-da-grizz": "C0329PXMA6T",
    "XXXXXXXXXXXX": "123456789",
    "XXXXXXXXXXXX": "123456789",
    "XXXXXXXXXXXX": "123456789",
    "XXXXXXXXXXXX": "123456789",
    
}


# Dates
base_date = "2025-07-14"  # week 1 start
# For each week, limit QS, QS-Q, 1stFQ, 3rdF, Donation, 2ndF, popup, to 1 occurrence.

# Test EC daily cap (1 EC event per day)
for i in range(4):
    add_row("2025-07-14","f3crossroads","C09BCTEBDE2",users["meta"],users["meta"],1,
            "EC test (only 1 EC per day)", "transform.py L155-166")

# Test workout Q (1stF with q_user_id == user_id)
add_row("2025-07-15","f3crossroads","C02B4U0M71N",users["meta"],users["meta"],1,
        "Workout Q test (1 point if not already Q that week)", "transform.py L109-122")

# Test Around the World bonus (5 unique AOs in one week)
ao_ids = ["C02B4U0M71N","C04PLDCTSJV","C04P7LRFBTM","C02DNLUQN9Y","C06SUFPUNTZ"]
for i, ao in enumerate(ao_ids):
    add_row(f"2025-07-{14+i}","f3crossroads",ao,users["meta"],users["meta"],1,
            "Around the World test (5 unique AOs in a week)", "transform.py L92-103")

# Test Sixpack bonus (6th and 7th posts in a week)
for i in range(6):
    add_row(f"2025-07-{14+i}","f3crossroads","C02B4U0M71N",users["chum"],users["chum"],1,
            "Sixpack bonus (6th post triggers +4, 7th adds +1)", "transform.py L105-120")

# Test QSource + QSource Q
add_row("2025-07-16","f3crossroads","C03KZ95AS75",users["villa"],users["villa"],1,
        "QSource points (once per week)", "transform.py L168-183")
add_row("2025-07-17","f3crossroads","C03KZ95AS75",users["villa"],users["villa"],1,
        "QSource Q points (once per week)", "transform.py L185-194")

# Test 2ndF, 3rdF, Donation, popup (weekly caps)
add_row("2025-07-18","f3crossroads","C017CTS0UQG",users["cutler"],users["cutler"],1,
        "2ndF weekly cap", "transform.py L197-205")
add_row("2025-07-18","f3crossroads","C0168ASEPD5",users["cutler"],users["cutler"],1,
        "3rdF weekly cap", "transform.py L207-213")
add_row("2025-07-18","f3crossroads","C0168ASEPD5",users["cutler"],users["cutler"],1,
        "Donation weekly cap", "transform.py L215-221")
add_row("2025-07-18","f3crossroads","C0168ASEPD5",users["cutler"],users["cutler"],1,
        "Popup weekly cap", "transform.py L223-229")

# Test hardshit cap (2 times only)
for i in range(3):
    add_row(f"2025-07-{18+i}","f3crossroads","C0168ASEPD5",users["boomer"],users["boomer"],1,
            "Hardshit cap test (only first 2 count)", "transform.py L231-238")

# Test Challenge Flag (multiple claims same day)
add_row("2025-07-14","f3crossroads","TBD0001FLAG",users["meta"],users["meta"],1,
        "Challenge flag claimed by Meta", "transform.py L303-333")
add_row("2025-07-14","f3crossroads","TBD0001FLAG",users["villa"],users["villa"],1,
        "Challenge flag also claimed by Villa (should get 0)", "transform.py L303-333")

# Test Santa Locks (5+ pax at same AO same day)
for uid in list(users.values())[:5]:
    add_row("2025-07-19","f3crossroads","C02AC8PS1T7",users["meta"],uid,1,
            "Santa Locks test (5 pax → 5 points)", "transform.py L345-356")

# Test CSAUP (≥8 pax vs <8 pax)
for uid in list(users.values())[:8]:
    add_row("2025-07-20","f3crossroads","TBD001CSAUP",users["villa"],uid,1,
            "CSAUP valid (8 pax → points)", "transform.py L373-385")
for uid in list(users.values())[:5]:
    add_row("2025-07-21","f3crossroads","TBD001CSAUP",users["villa"],uid,1,
            "CSAUP invalid (<8 pax → 0 points)", "transform.py L373-385")

# Test FNG1, FNG5, FNGQ
for i in range(5):
    add_row(f"2025-07-{14+i}","f3crossroads","C02B4U0M71N",users["fng1"],users["fng1"],1,
            "FNG1 on first post, FNG5 on fifth post", "transform.py L402-428")
add_row("2025-07-19","f3crossroads","C02B4U0M71N",users["fng1"],users["fng1"],1,
        "FNGQ (FNG Qing workout)", "transform.py L430-438")

# Test Unknown user (not in PAXdraft or PAXcurrent)
add_row("2025-07-22","f3crossroads","C02B4U0M71N","UNKNOWN_Q","UNKNOWN_USER",1,
        "Unknown user should map to Unknown Name/Team", "transform.py L41-56")

# Build DataFrame
df_test = pd.DataFrame(rows)

# Save to CSV
output_path = "/mnt/data/postdata_test.csv"
df_test.to_csv(output_path, index=False)



