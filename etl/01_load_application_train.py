import pandas as pd
from sqlalchemy import create_engine, text

CSV_PATH = r"C:\Users\HP\OneDrive\Desktop\CreditRiskProject\application_train.csv"  
DB_URL   = "postgresql+psycopg2://test_user:root@localhost:5432/mydb"                   

engine = create_engine(DB_URL, future=True)

# Make the script repeatable
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS staging.stg_application_train;"))

rows = 0
first = True
for chunk in pd.read_csv(CSV_PATH, chunksize=50000, low_memory=False):
    # normalize column names a bit
    chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
    chunk.to_sql(
        "stg_application_train",
        engine,
        schema="staging",
        if_exists="replace" if first else "append",
        index=False,
        method="multi",
        chunksize=50000
    )
    rows += len(chunk)
    first = False
    print(f"Loaded {rows} rows...")

print(f"✅ Done. Total rows: {rows}")
