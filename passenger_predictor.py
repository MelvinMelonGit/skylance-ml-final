#!/usr/bin/env python
# coding: utf-8

# In[1]:


# passenger_predictor.py
import os, pickle, pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from config import DB_URL

# Purpose: ETL + ML + write-back

# 1) Load your DB_URL from the environment
db_url = os.getenv("DB_URL") # call from config.py
if not db_url:
    raise RuntimeError("Please set DB_URL in your environment")


# 2) Create engine & session
def get_engine_and_session():
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return engine, Session()

# 3) Create load pipeline path
def load_pipeline(path="rf_pipeline.pkl"):
    with open(path, "rb") as f:
        return pickle.load(f)
    
# --- ETL & PREDICT FUNCTIONS ---
def fetch_features(engine, booking_id:str = None) -> pd.DataFrame:
    # 4) Pull exactly once with your big JOIN
    join_sql = """
    SELECT
      fbd.Id                AS BookingID,
      ac.Airline,
      o.IataCode    AS Origin,
      d.IataCode    AS Destination,
      fd.FlightStatus AS Flight_Status,
      YEAR(fd.DepartureTime) - YEAR(au.DateOfBirth) AS Age,
      au.Gender,
      fbd.TravelPurpose AS Travel_Purpose,
      au.MembershipTier,
      fd.Distance      AS Distance_km,
      fd.IsHoliday,
      HOUR(fd.DepartureTime)      AS Departure_Hour,
      DAYOFWEEK(fd.DepartureTime)-1 AS Departure_Weekday,
      MONTH(fd.DepartureTime)     AS Departure_Month,
      fbd.Fareamount    AS Price,
      fbd.BaggageAllowance
    FROM flightbookingdetails fbd
    JOIN flightdetails    fd ON fd.Id = fbd.FlightDetailId
    JOIN aircraft         ac ON ac.Id = fd.AircraftId
    JOIN airports         o  ON o.Id  = fd.OriginAirportId
    JOIN airports         d  ON d.Id  = fd.DestinationAirportId
    JOIN bookingdetails   bd ON bd.Id = fbd.BookingDetailId
    JOIN appusers         au ON au.Id = bd.AppUserId
    WHERE fbd.Prediction IS NULL
        AND fbd.Id = %s
    """

    # raw_connection so pandas.cursor() works
    conn = engine.raw_connection()
    try:
        df = pd.read_sql_query(join_sql, conn, params=[booking_id])
    finally:
        conn.close()
    if df.empty:
        return df

    # 5) Derive the engineered ones with existing (if needed)
    # 5a) Map gender codes back to full strings
    df['Gender'] = df['Gender'].map({
        'M': 'Male',
        'F': 'Female'
    })

    # 5b) Map the numeric travel-purpose codes to the original labels
    df['Travel_Purpose'] = df['Travel_Purpose'].map({
        0: 'Business',
        1: 'Family',
        2: 'Leisure',
        3: 'Emergency'
    })

    # 5c) Fix the “Normal” label back to “None”
    df['MembershipTier'] = df['MembershipTier'].replace({
        'Normal': 'None'
    })

    # 6) Fill in the missing features with sensible defaults:
    df['Seat_Class']              = 'Economy'
    df['Check_in_Method']         = 'Online'
    df['Flight_Status']         = 'On-time'
    df['Delay_Minutes']           = 0.0
    df['Booking_Days_In_Advance'] = 0
    df['Weather_Impact']          = 0

    # 7) Rename columns to match the pipeline (if needed)

    return df

# 8) Slice out exactly what the pipeline expects
feature_cols = [
    'Airline','Origin','Destination','Flight_Status',
    'Age','Gender','Travel_Purpose','Seat_Class',
    'MembershipTier','Check_in_Method','Delay_Minutes',
    'Booking_Days_In_Advance','Weather_Impact','Distance_km',
    'IsHoliday','Departure_Hour','Departure_Weekday',
    'Departure_Month','Price','BaggageAllowance'
]

# 9) Predict + write-back: handles both single & multi
def predict_and_write_back(engine, session, pipeline, df: pd.DataFrame):
    X = df[feature_cols]
    preds = pipeline.predict(X)

    # Reflect the table for updates
    meta = MetaData()
    fbd_tbl = Table("flightbookingdetails", meta, autoload_with=engine)
    
    for booking_id, pred in zip(df["BookingID"], preds):
        stmt = (
            update(fbd_tbl)
            .where(fbd_tbl.c.Id == booking_id)
            .values(Prediction=int(pred)) # writeback 0 and 1 to db
        )
        session.execute(stmt)
    session.commit()
    
    # always return an int
    return int(preds[0]) if len(preds) == 1 else len(preds)


# In[2]:


# # Purpose: ETL + ML + write-back

# # 1) Load your DB_URL from the environment
# db_url = os.getenv("DB_URL") # call from config.py
# if not db_url:
#     raise RuntimeError("Please set DB_URL in your environment")


# # 2) Create engine & session
# def get_engine_and_session():
#     engine = create_engine(db_url)
#     Session = sessionmaker(bind=engine)
#     return engine, Session()

# # 3) Create load pipeline path
# def load_pipeline(path="rf_pipeline.pkl"):
#     with open(path, "rb") as f:
#         return pickle.load(f)
    
# # --- ETL & PREDICT FUNCTIONS ---
# def fetch_features(engine, booking_id: int = None) -> pd.DataFrame:
#     # 4) Pull exactly once with your big JOIN
#     join_sql = """
#     SELECT
#       fbd.Id                AS BookingID,
#       ac.Airline,
#       o.IataCode    AS Origin,
#       d.IataCode    AS Destination,
#       fd.FlightStatus AS Flight_Status,
#       YEAR(fd.DepartureTime) - YEAR(au.DateOfBirth) AS Age,
#       au.Gender,
#       fbd.TravelPurpose AS Travel_Purpose,
#       au.MembershipTier,
#       fd.Distance      AS Distance_km,
#       fd.IsHoliday,
#       HOUR(fd.DepartureTime)      AS Departure_Hour,
#       DAYOFWEEK(fd.DepartureTime)-1 AS Departure_Weekday,
#       MONTH(fd.DepartureTime)     AS Departure_Month,
#       fbd.Fareamount    AS Price,
#       fbd.BaggageAllowance
#     FROM flightbookingdetails fbd
#     JOIN flightdetails    fd ON fd.Id = fbd.FlightDetailId
#     JOIN aircraft         ac ON ac.Id = fd.AircraftId
#     JOIN airports         o  ON o.Id  = fd.OriginAirportId
#     JOIN airports         d  ON d.Id  = fd.DestinationAirportId
#     JOIN bookingdetails   bd ON bd.Id = fbd.BookingDetailId
#     JOIN appusers         au ON au.Id = bd.AppUserId
#     WHERE fbd.Prediction IS NULL
#     """
#     # Dynamic filtering
#     # The same function can either grab all un-predicted rows 
#     # (when booking_id is None) 
#     # or just one (when pass an id only), 
#     # without copying two separate queries.
#     params = {}
#     if booking_id is not None:
#         join_sql += " AND fbd.Id = :bid"
#         params["bid"] = booking_id

#     conn = engine.raw_connection()
#     try:
#         df = pd.read_sql_query(join_sql, conn,
#             params=params)
#     finally:
#         conn.close()

#     if df.empty:
#         return df    # hand back an empty DataFrame

#     # 5) Derive the engineered ones with existing (if needed)
#     # 5a) Map gender codes back to full strings
#     df['Gender'] = df['Gender'].map({
#         'M': 'Male',
#         'F': 'Female'
#     })

#     # 5b) Map the numeric travel-purpose codes to the original labels
#     df['Travel_Purpose'] = df['Travel_Purpose'].map({
#         0: 'Business',
#         1: 'Family',
#         2: 'Leisure',
#         3: 'Emergency'
#     })

#     # 5c) Fix the “Normal” label back to “None”
#     df['MembershipTier'] = df['MembershipTier'].replace({
#         'Normal': 'None'
#     })

#     # 6) Fill in the missing features with sensible defaults:
#     df['Seat_Class']              = 'Economy'
#     df['Check_in_Method']         = 'Online'
#     df['Flight_Status']         = 'On-time'
#     df['Delay_Minutes']           = 0.0
#     df['Booking_Days_In_Advance'] = 0
#     df['Weather_Impact']          = 0

#     # 7) Rename columns to match the pipeline (if needed)

#     return df

# # 8) Slice out exactly what the pipeline expects
# feature_cols = [
#     'Airline','Origin','Destination','Flight_Status',
#     'Age','Gender','Travel_Purpose','Seat_Class',
#     'MembershipTier','Check_in_Method','Delay_Minutes',
#     'Booking_Days_In_Advance','Weather_Impact','Distance_km',
#     'IsHoliday','Departure_Hour','Departure_Weekday',
#     'Departure_Month','Price','BaggageAllowance'
# ]

# # 9) Predict + write-back: handles both single & multi
# def predict_and_write_back(engine, session, pipeline, df: pd.DataFrame):
#     X = df[feature_cols]
#     preds = pipeline.predict(X)

#     # Reflect the table for updates
#     meta = MetaData()
#     fbd_tbl = Table("flightbookingdetails", meta, autoload_with=engine)
    
#     for booking_id, pred in zip(df["BookingID"], preds):
#         stmt = (
#             update(fbd_tbl)
#             .where(fbd_tbl.c.Id == booking_id)
#             .values(Prediction=int(pred)) # writeback 0 and 1 to db
#         )
#         session.execute(stmt)
#     session.commit()
    
#     # always return an int
#     return int(preds[0]) if len(preds) == 1 else len(preds)


# In[ ]:




