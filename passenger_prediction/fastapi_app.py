#!/usr/bin/env python
# coding: utf-8

# In[4]:


# fastapi_app.py
import os
from config import DB_URL

from fastapi import FastAPI, HTTPException
from passenger_predictor import get_engine_and_session, load_pipeline, fetch_features, predict_and_write_back

app = FastAPI()
engine, session = get_engine_and_session()
pipeline = load_pipeline()

@app.post("/predict")
def predict_all():
    df = fetch_features(engine)
    if df.empty:
        return {"updated": 0}
    updated = predict_and_write_back(engine, session, pipeline, df) if not df.empty else 0
    return {"updated": updated}

@app.post("/predict/{booking_id}")
def predict_single(booking_id: str):
    df = fetch_features(engine, booking_id)    # returns a 1-row DataFrame
    if df.empty:
        raise HTTPException(404, "Booking not found or already predicted")
    pred = predict_and_write_back(engine, session, pipeline, df)
    return {"booking_id": booking_id, "prediction": pred}


# In[ ]:




