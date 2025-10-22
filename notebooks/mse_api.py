# Week 3: API Development
# Implement all 3-5 required API endpoints
# Add input validation using Pydantic models
# Add query parameters for filtering

# create a virtual environment 
# python -m venv venv
# activate the virtual environment  
# .\venv\Scripts\activate
# install required packages
# pip install -r requirements.txt
# run py.script(venv) D:\Documents\AIMS_DSCBI_Training\mse-api-assignment>python -m uvicorn api_access:app --reload


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
# RETURNING DATA FROM POSTGRESQL MSE_DATABASE
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
# HELPER FUNCTION TO CONNECT QUERY to sql database
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
# set ENDPOINTS and use app (fastAPI) to create a link form endpoints and convert sql database and retrieve API data
# ===========================================================================================
# link : http://127.0.0.1:8000 
@app.get("/")
def Home():
    return {"message":"WELCOME TO MALAWI STOCK EXCHANGE DATABASE"}

@app.get("/companies")
def companies():
    sql = "SELECT * FROM tickers"
    return run_query(sql)

# Get companies by sector
@app.get("/companies/{sector}")
def get_companies(sector: str):
    """These are all company related data including counter_id, counter, listing price and Listing date
    To retrieve the sector related info, plase do the following....
    Company/sector=?"""

  # Get all API from sql tickers database
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
    LIMIT 50;
    """
    return run_query(sql, (counter,))

# create an endpoints to get daily-prices API
# Get all daily-prices API from sql daily_prices database
@app.get("/daily_prices")
def daily_prices():
    sql = "SELECT * FROM daily_prices"
    return run_query(sql)

@app.get("/companies/{counter}")
def get_counter(counter: str):
    sql = """
    SELECT
    
        t.counter,
        t."daily_range_high",
        t."daily_range_low",
        t."buy_price",
        t."sell_price",
        t."previous_closing_price",
        t."today_closing_price",
        t."volume_traded",
        COUNT(d.counter_id) AS price_entries,
        d.counter_id AS price_counter_id
    FROM daily_prices AS t
    LEFT JOIN Daily_prices AS d 
        ON t.counter_id = d.counter_id
    WHERE LOWER(t.counter) = LOWER(%s)
    GROUP BY
        t.counter_id,
        t.counter,
    LIMIT 50;
    """
    return run_query(sql, (counter,))



@app.get("/prices/daily")
def get_daily_prices(
    ticker: str = Query(..., description="Stock ticker symbol (e.g., NICO)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, description="Maximum number of records to return (default: 100, max: 1000)")
):
    # --- validate limit ---
    limit = min(limit, 1000)

    # --- build dynamic SQL ---
    sql = """
    SELECT
        counter_id,
        counter,
        trade_date,
        daily_range_high,
        daily_range_low,
        buy_price,
        sell_price,
        previous_closing_price,
        today_closing_price,
        volume_traded
    FROM daily_prices
    WHERE LOWER(counter) = LOWER(%s)
    """
    params = [ticker]




