#!/usr/bin/env python
# coding: utf-8

# In[3]:


# flight_predictor.py
import os, pickle, pandas as pd
from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy.orm import sessionmaker
from config import DB_URL

db_url = os.getenv("DB_URL")
if not db_url:
    raise RuntimeError("Please set DB_URL in your environment")

def get_engine_and_session():
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return engine, Session()

def load_pipeline(path="flight_no_show_pipeline.pkl"):
    with open(path, "rb") as f:
        return pickle.load(f)

def fetch_features(engine, flight_id: int = None) -> pd.DataFrame:
    sql = """
    SELECT
      fd.Id                            AS FlightID,
      ac.Airline,
      ac.SeatCapacity                  AS Flight_Capacity,
      o.IataCode                       AS Origin,
      d.IataCode                       AS Destination,
      fd.Distance                      AS Distance_km,
      fd.SeatsSold                     AS Total_Seats_Sold,
      (fd.SeatsSold - fd.CheckInCount) AS Total_No_Show,
      TIMESTAMPDIFF(
        MINUTE, fd.DepartureTime, fd.ArrivalTime
      )                                AS FlightDurationMinutes,
      MONTH(fd.DepartureTime)          AS Departure_Month,
      DAYOFWEEK(fd.DepartureTime) - 1  AS Departure_Weekday,
      HOUR(fd.DepartureTime)           AS Departure_Hour
    FROM FlightDetails fd
    LEFT JOIN FlightBbookingDetails fbd      ON fbd.FlightDetailId = fd.Id
    JOIN Aircraft ac                   ON ac.Id  = fd.AircraftId
    JOIN Airports o                    ON o.Id   = fd.OriginAirportId
    JOIN Airports d                    ON d.Id   = fd.DestinationAirportId
    WHERE fd.Probability IS NULL
    """
    params = []
    if flight_id is not None:
        sql += " AND fd.Id = %s"
        params = [flight_id]

    conn = engine.raw_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    # fill in global‐mean defaults
    df["Frequent_Flyer_Ratio"] = 0.25
    df["Last_Min_Bookings"]    = 60
    df["Weather_Impact"]       = 0

    return df

feature_cols = [
    "Distance_km", "FlightDurationMinutes", "Flight_Capacity", "Total_Seats_Sold",
    "Frequent_Flyer_Ratio", "Last_Min_Bookings", "Total_No_Show",
    "Departure_Month", "Departure_Weekday", "Departure_Hour",
    "Weather_Impact", "Airline", "Origin", "Destination"
]

def predict_and_write_back(engine, session, pipeline, df: pd.DataFrame):
    X = df[feature_cols]
    raw_preds = 1 - pipeline.predict(X)
    preds = [round(p * 100, 2) for p in raw_preds]

    meta   = MetaData()
    fd_tbl = Table("flightdetails", meta, autoload_with=engine)

    for flight_id, pred in zip(df["FlightID"], preds):
        stmt = (
            update(fd_tbl)
            .where(fd_tbl.c.Id == flight_id)
            .values(Probability=pred) 
        )
        session.execute(stmt)

    session.commit()
    return preds[0] if len(preds) == 1 else len(preds)


# In[ ]:




