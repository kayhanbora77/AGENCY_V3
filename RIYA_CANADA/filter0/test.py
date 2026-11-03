import pandas as pd
from pathlib import Path

path = r"C:\Users\cagri\Desktop\Group2_2026_September\RiyaCanada\filter-0\Canada_AllData.xlsx"

xl = pd.ExcelFile(path, engine="openpyxl")
print("Sheets:", xl.sheet_names)

for sh in xl.sheet_names:
    # peek the first 6 raw rows WITHOUT headers
    raw = xl.parse(sh, header=None, nrows=6, dtype=str)
    print(f"\n=== sheet {sh} — first 6 rows, no header ===")
    print(raw.to_string(max_colwidth=25))

    # and what pandas sees with default header=0
    df = xl.parse(sh, header=0, nrows=2, dtype=str)
    print(f"\n--- sheet {sh} — columns with header=0 ---")
    print(list(df.columns))