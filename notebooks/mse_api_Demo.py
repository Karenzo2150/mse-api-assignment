from fastapi import FastAPI, Query, Path
from typing import Optional
from datetime import date
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv()

PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

print("PostgreSQL Connection Settings:")
print(f"Host: {PGHOST}")
print(f"Port: {PGPORT}")
print(f"Database: {PGDATABASE}")
print(f"User: {PGUSER}")
print(f"Password: {'[SET]' if PGPASSWORD else '[NOT SET]'}")

connection_string = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
print("Connection psql string:", connection_string)

engine = create_engine(connection_string, pool_pre_ping=True)

app = FastAPI()

sector_map = {
    'AIRTEL': 'Telecommunication',
    'BHL': 'Hospitality',
    'FDHB': 'Finance',
    'FMBCH': 'Finance',
    'ICON': 'Construction',
    'ILLOVO': 'Agriculture',
    'MPICO': 'Construction',
    'NBM': 'Finance',
    'NBS': 'Finance',
    'NICO': 'Finance',
    'NITL': 'Finance',
    'OMU': 'Finance',
    'PCL': 'Investments',
    'STANDARD': 'Finance',
    'SUNBIRD': 'Hospitality',
    'TNM': 'Telecommunication'
}
#First end-point: with Query parameters
@app.get("/companies")
def get_companies(sector: Optional[str] = Query(None, description="Filter companies by sector")):
    df=pd.read_sql("SELECT * FROM counters", con=engine)
    df['Sector'] = df['ticker'].map(sector_map)
    df=df[['ticker','name','Sector','date_listed']]
    if sector:
        df=df[df['Sector']==sector]
    data=df.to_dict(orient='records')
    return {'count':len(data), 'data':data}

#Second end-point - with Path parameters
@app.get("/companies/{ticker}")
def get_company_details(ticker:str):
    #counter details
    df1=pd.read_sql("SELECT * FROM counters", con=engine)
    df1['Sector'] = df1['ticker'].map(sector_map)
    df1=df1[df1['ticker']==ticker]
    id=df1['counter_id'].values[0]
    df1=df1[['ticker','name','Sector','date_listed']]
    company_details=df1.to_dict(orient='records')

    #get counter records from daily prices
    df2=pd.read_sql("SELECT * FROM prices", con=engine)
    df2=df2[df2['counter_id']==id]
    records=len(df2)
    return {'Company details':company_details,'Total records':records}

#Third end-point
@app.get("/prices/daily")
def get_daily_prices(
    ticker: str = Query(..., description="Stock ticker symbol"),
    start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(100, description="Maximum records to return")
    ):
    #fetch counter name from counter table
    df1=pd.read_sql("SELECT * FROM counters", con=engine)
    df1=df1[df1['ticker']==ticker]
    id=df1['counter_id'].values[0]
    
    #filter ticker using counter id
    df=pd.read_sql("SELECT * FROM prices", con=engine)
    df = df[df['counter_id'] == id]

    df=df[['open_mwk','high_mwk','low_mwk','close_mwk','volume','trade_date']]
    df.columns=['open',' high','low','close','volume','trade_date']

    #df = df[df['ticker'] == ticker]

    # Filter by date range
    if start_date:
        df = df[df['trade_date'] >= start_date]
    if end_date:
        df = df[df['trade_date'] <= end_date]

    # Apply limit (max 1000)
    limit = min(limit or 100, 1000)
    df = df.head(limit)
    df = df.fillna('') 
    return {"Company": ticker, "data":df.to_dict(orient='records')}
#Fouth end-point
@app.get("/prices/range")
def get_daily_prices(
    ticker: str = Query(..., description="Stock ticker symbol"),
    year: int = Query(..., description="Year"),
    month: Optional[int] = Query(None, description="Month of the year"),
    ):
    #fetch counter name from counter table
    df1=pd.read_sql("SELECT * FROM counters", con=engine)
    df1=df1[df1['ticker']==ticker]
    id=df1['counter_id'].values[0]
    
    #filter ticker using counter id
    df=pd.read_sql("SELECT * FROM prices", con=engine)
    df = df[df['counter_id'] == id]

    df=df[['trade_date','open_mwk','high_mwk','low_mwk','close_mwk','volume']]
    df.columns=['Period','open',' high','low','close','Total Volume']

    df['Period'] = pd.to_datetime(df['Period'])
    df = df[df['Period'].dt.year == year]
    if month:
        df = df[df['Period'].dt.month == month]

    df = df.fillna('') 
    return {"Company": ticker, "data":df.to_dict(orient='records')}

#Fith end-point
@app.get("/prices/latest")
def get_recent_prices(
    ticker: Optional[str] = Query(None, description="Stock ticker symbol"),
    ):
     #fetch counter name from counter table
    df1=pd.read_sql("SELECT * FROM counters", con=engine)
    df1=df1[df1['ticker']==ticker]
    id=df1['counter_id'].values[0]
    
    #filter ticker using counter id
    df=pd.read_sql("SELECT * FROM prices", con=engine)
    df = df[df['counter_id'] == id]

    df=df[['trade_date','open_mwk','high_mwk','low_mwk','close_mwk','volume']]
    df.columns=['trade_date','open',' high','low','close','Total Volume']

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df_prices = df.sort_values(by='trade_date', ascending=False).reset_index(drop=True)

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
    uvicorn.run("data_api:app", host="127.0.0.1", port=8000, reload=True)
