#!/usr/bin/env python
# coding: utf-8

# In[3]:


# fastapi_flight.py

import os
from config import DB_URL

from fastapi import FastAPI, HTTPException
from flight_predictor import get_engine_and_session, load_pipeline, fetch_features, predict_and_write_back

app = FastAPI()
engine, session = get_engine_and_session()
pipeline = load_pipeline()

@app.post("/predict_f")
def predict_flights():
    df = fetch_features(engine)
    if df.empty:
        return {"updated": 0}
    updated = predict_and_write_back(engine, session, pipeline, df) if not df.empty else 0
    return {"updated": updated}

@app.post("/predict_f/{flight_id}")
def predict_flight(flight_id: int):
    df = fetch_features(engine, flight_id)    # returns a 1-row DataFrame
    if df.empty:
        raise HTTPException(404, "Flight not found or already predicted")
    pred = predict_and_write_back(engine, session, pipeline, df)
    return {"flight_id": flight_id, "probability": pred}


# In[ ]:





# In[ ]:





# In[ ]:




