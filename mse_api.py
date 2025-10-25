import os
from typing import Optional, List
import pandas as pd
import psycopg2
from fastapi import FastAPI, Query, Path, HTTPException
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import numpy as np
import uvicorn
from datetime import date
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
PGPASSWORD = os.getenv("PGPASSWORD", "").strip()

# =============================================================
# Connect to the PostgreSQL database
# =============================================================
def run_query(sql: str, params: tuple = ()):
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER)
    try:
        df = pd.read_sql(sql, conn, params=params)
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None}) # convert NaN to none
    finally:
        conn.close()
    return df.to_dict(orient = "records")

connect = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
print("Connection psql string:", connect)

engine = create_engine(connect, pool_pre_ping = True)
# =============================================================
# ENDPOINTS (NO DATA MODEL)
# =============================================================
# Home page
@app.get("/")
def Home():
    return {"message":"WELCOME TO MALAWI STOCK EXCHANGE DATABASE"}

    sql = "SELECT * FROM tickers"
    return run_query(sql)

# 1. GET /companies
@app.get("/companies")
def get_companies(sector: Optional[str] = Query(None, description = "Filter companies by sector")):
    sql = pd.read_sql("SELECT * FROM tickers", con = engine)
    #sql['sector'] = sql['ticker'].map(sector_map)
    sql = sql[['ticker','name','sector','date_listed']]
    if sector:
        sql = sql[sql['sector'] == sector]
    sql_dict = sql.to_dict(orient='records')
    return {'count':len(sql_dict), 'data':sql_dict}

# 2. GET /companies/{ticker}
@app.get("/companies/{ticker}")
def get_ticker_info(ticker:str):
    
    sql1 = pd.read_sql("SELECT * FROM tickers", con = engine)
    sql1 = sql1[sql1['ticker'] == ticker]
    id   =  sql1['counter_id'].values[0]
    sql1 = sql1[['ticker','name','sector','date_listed']]
    ticker_info = sql1.to_dict(orient = 'records')

    sql2 = pd.read_sql(f"SELECT * FROM daily_prices", con = engine)
    sql2 = sql2 [sql2['counter_id'] == id]
    records = len(sql2)
    return {'Company details':ticker_info,'Total records':records}

# 3. GET /prices/daily

@app.get("/prices/daily")
def daily_prices_ticker(
    ticker: str = Query(..., description="Stock ticker symbol"),
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(100, description="Maximum records to return")
    ):
    #fetch counter name from tickers table
    sql0 = pd.read_sql("SELECT * FROM tickers", con = engine)
    sql0  = sql0[sql0['ticker'] == ticker]
    id = sql0['counter_id'].values[0]
    
    #filter ticker using counter id
    sql3 = pd.read_sql("SELECT * FROM daily_prices", con = engine)
    sql3 = sql3[sql3['counter_id'] == id]

    if start_date:
        sql3 = sql3[sql3['trade_date'] >= start_date]
    if end_date:
        sql3 = sql3[sql3['trade_date'] <= end_date]

    # Apply limit (max 1000)
    limit = min(limit or 100, 1000)
    sql3 = sql3.head(limit)
    sql3 = sql3.fillna('') 
    return {"Name": ticker, "data":sql3.to_dict(orient='records')}

#Fouth end-point
@app.get("/prices/range")
def prices_range(
    ticker: str = Query(..., description = "Stock ticker symbol"),
    year: int = Query(..., description = "Year"),
    month: Optional[int] = Query(None, description = "Month of the year"),
    ):
    #fetch counter name from counter table
    df1 = pd.read_sql("SELECT * FROM tickers", con = engine)
    df1 = df1[df1['ticker']==ticker]
    id = df1['counter_id'].values[0]
     
    #filter ticker using counter id
    df = pd.read_sql("SELECT * FROM daily_prices", con = engine)
    df = df[df['counter_id'] == id]

    df = df[['trade_date','open_mwk','high_mwk','low_mwk','close_mwk','volume']]
    df.columns=['Period','open',' high','low','close','Total Volume']

    df['Period'] = pd.to_datetime(df['Period'])
    df = df[df['Period'].dt.year == year]
    if month:
        df = df[df['Period'].dt.month == month]

    df = df.fillna('') 
    return {"Company": ticker, "data":df.to_dict(orient='records')}

#Fith end-point
@app.get("/prices/latest")
def recent_prices(
    ticker: Optional[str] = Query(None, description="Stock ticker symbol"),
    ):
     #fetch counter name from counter table
    sql = pd.read_sql("SELECT * FROM tickers", con=engine)
    sql = sql[sql['ticker']==ticker]
    id = sql['counter_id'].values[0]
    
    #filter ticker using counter id
    df = pd.read_sql("SELECT * FROM daily_prices", con=engine)
    df = df[df['counter_id'] == id]

    df = df[['trade_date','open_mwk','high_mwk','low_mwk','close_mwk','volume']]
    df.columns = ['trade_date','open',' high','low','close','Total Volume']

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df_prices = df.sort_values(by ='trade_date', ascending = False).reset_index(drop=True)

    # Latest price
    latest = df_prices.iloc[0]
    latest_date = latest['trade_date']
    latest_price = latest['close']
    
    # Previous price
    if len(df_prices) > 1:
        prev_price = df_prices.iloc[1]['close']
        change = latest_price - prev_price
        change_percentage = (change / prev_price) * 100 if prev_price != 0 else 0
    else:
        prev_price = None
        change = None
        change_percentage = None
    return {
        "ticker": ticker,
        "latest_date": latest_date,
        "latest_price": latest_price,
        "previous_price": prev_price,
        "change": change,
        "change_percentage": str(round(change_percentage,3))+'%'
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mse_api:app", host="127.0.0.1", port=8000, reload=True)