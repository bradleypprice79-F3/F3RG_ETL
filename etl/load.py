# etl/load.py
import pandas as pd
import os

def to_csv(df, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    # I had the below, but it writes the csv with an ending newline.  This causes an empty row in javascript.
    # df.to_csv(filepath, index=False)

    # this method of writing writes without the new line and therefore gets rid of the missing data in javascript.
    csv_text = df.to_csv(index=False, na_rep="<NA>").rstrip("\r\n")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        f.write(csv_text)
    print(f"Saved CSV to {filepath}")




def to_html(html_content, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Saved HTML report to {filepath}")