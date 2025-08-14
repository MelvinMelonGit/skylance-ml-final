#!/usr/bin/env python
# coding: utf-8

# In[3]:


# fastapi_flight.py

import os
from config import DB_URL

from fastapi import FastAPI, HTTPException
from fastapi import HTTPException
import traceback
from .flight_predictor import get_engine_and_session, load_pipeline, fetch_features, predict_and_write_back

app = FastAPI()
engine, session = get_engine_and_session()
pipeline = load_pipeline()

@app.post("/predict_f")
def predict_flights():
    try:
        df = fetch_features(engine)
        if df.empty:
            return {"updated": 0}

        updated = predict_and_write_back(engine, session, pipeline, df)
        return {"updated": updated}

    except Exception as e:
        # Print full traceback to logs for debugging
        print(f"[ERROR] Flight batch prediction failed: {e}")
        traceback.print_exc()
        # Return 500 to client with error detail
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict_f/{flight_id}")
def predict_flight(flight_id: int):
    try:
        df = fetch_features(engine, flight_id) # returns a 1-row DataFrame
        if df.empty:
            raise HTTPException(404, "Flight not found or already predicted")
        pred = predict_and_write_back(engine, session, pipeline, df)
        return {"flight_id": flight_id, "probability": pred}
    except Exception as e:
        print(f"[ERROR] Flight prediction failed for ID {flight_id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# In[ ]:





# In[ ]:





# In[ ]:




