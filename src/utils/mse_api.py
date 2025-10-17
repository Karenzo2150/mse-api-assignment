import os
from typing import Optional, List
import pandas as pd
import psycopg2
from fastapi import FastAPI, Query, Path, HTTPException
from dotenv import load_dotenv
from pathlib import Path
import numpy as np
load_dotenv()


app = FastAPI()


# ==========================================================
# RETURNING DATA FROM POSTGRESQL MSE_LAB DATABASE
# =========================================================

# =============================================================
# Load environment variables from .env file
# =============================================================
load_dotenv()

# Get database connection details from environment variables
PGHOST = os.getenv("PGHOST", "").strip()
PGPORT = os.getenv("PGPORT", "").strip()
PGPORT = int(''.join(filter(str.isdigit, PGPORT))) if PGPORT else 5432
PGDATABASE = os.getenv("PGDATABASE", "").strip()
PGUSER = os.getenv("PGUSER", "").strip()

# =============================================================
# HELPER FUNCTION TO CONNECT AND QUERY
# =============================================================
def run_query(sql: str, params: tuple = ()):
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
       
    )
    try:
        df = pd.read_sql(sql, conn, params=params)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None}) # convert NaN to none
    finally:
        conn.close()
    return df.to_dict(orient = "records")

# =============================================================
# ENDPOINTS (NO DATA MODEL)
# =============================================================

@app.get("/")
def Home():
    return {"message":"WELCOME TO MALAWI STOCK EXCHANGE DATABASE"}

    sql = "SELECT * FROM tickers"
    return run_query(sql)


@app.get("/companies/{sector}")
def get_companies(sector: str):
    """These are all company related data including counter_id, counter, listing price and Listing date
    To retrieve the sector related info, plase do the following....
    Company/sector=?"""

  # Get all tickers
    sql = 'SELECT * FROM tickers WHERE LOWER("Sector") = %s'
    return run_query(sql, (sector.lower(),))
    
@app.get("/companies/{counter}")
def get_company_prices(counter: str):
    sql = """
    SELECT
        t.counter,
        t.name,
        t."Sector",
        t."Date Listed",
        COUNT(d.counter_id) AS price_entries,
        d.counter_id AS price_counter_id
    FROM tickers AS t
    LEFT JOIN Daily_prices AS d 
        ON t.counter_id = d.counter_id
    WHERE LOWER(t.counter) = LOWER(%s)
    GROUP BY
        t.counter,
        t.name,
        t."Sector",
        t."Date Listed"
    LIMIT 10;
    """
    return run_query(sql, (counter,))


    

@app.get("/daily prices")
def daily_prices():
    sql = "SELECT * FROM Daily_prices"
    return run_query(sql)










