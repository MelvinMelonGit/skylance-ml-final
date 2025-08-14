#!/usr/bin/env python
# coding: utf-8

# In[4]:


# fastapi_app.py
import os
from config import DB_URL

from fastapi import FastAPI, HTTPException
from fastapi import HTTPException
import traceback
from .passenger_predictor import get_engine_and_session, load_pipeline, fetch_features, predict_and_write_back

app = FastAPI()
engine, session = get_engine_and_session()
pipeline = load_pipeline()

@app.post("/predict")
def predict_all():
    try:
        df = fetch_features(engine)
        if df.empty:
            return {"updated": 0}

        updated = predict_and_write_back(engine, session, pipeline, df)
        return {"updated": updated}

    except Exception as e:
        print(f"[ERROR] Batch booking prediction failed: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/{booking_id}")
def predict_single(booking_id: str):
    try:
        df = fetch_features(engine, booking_id)  # returns a 1-row DataFrame
        if df.empty:
            raise HTTPException(404, "Booking not found or already predicted")

        pred = predict_and_write_back(engine, session, pipeline, df)
        return {"booking_id": booking_id, "prediction": pred}

    except HTTPException:
        # Re-raise HTTPException (like 404) without logging again
        raise
    except Exception as e:
        print(f"[ERROR] Single booking prediction failed for ID {booking_id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# In[ ]:




