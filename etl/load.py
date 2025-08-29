# etl/load.py
import pandas as pd
import os

def to_csv(df, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    print(f"Saved CSV to {filepath}")

def to_html(html_content, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Saved HTML report to {filepath}")