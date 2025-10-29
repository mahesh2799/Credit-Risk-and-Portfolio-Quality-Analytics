import pandas as pd
from sqlalchemy import create_engine, text

# --- EDIT THESE TWO LINES ---
CSV_PATH = r"C:\Users\HP\OneDrive\Desktop\CreditRiskProject\bureau.csv"  
DB_URL   = "postgresql+psycopg2://test_user:root@localhost:5432/mydb"            

engine = create_engine(DB_URL, future=True)

TABLE = "stg_bureau"
SCHEMA = "staging"

# Making script repeatable
with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE};"))

rows = 0
first = True
for chunk in pd.read_csv(CSV_PATH, chunksize=50000, low_memory=False):
    # normalize column names
    chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
    chunk.to_sql(
        TABLE,
        engine,
        schema=SCHEMA,
        if_exists="replace" if first else "append",
        index=False,
        method="multi",
        chunksize=50000
    )
    rows += len(chunk)
    first = False
    print(f"Loaded {rows} rows...")

print(f"✅ Done loading {SCHEMA}.{TABLE}. Total rows: {rows}")
