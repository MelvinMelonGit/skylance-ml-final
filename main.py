from fastapi import FastAPI
from passenger_prediction.fastapi_app import app as passenger_app
from flight_prediction.fastapi_flight import app as flight_app

app = FastAPI()

app.mount("/passenger", passenger_app)
app.mount("/flight", flight_app)