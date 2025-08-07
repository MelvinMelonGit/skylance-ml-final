FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc build-essential default-libmysqlclient-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY rf_pipeline.pkl .

EXPOSE 8000

CMD ["uvicorn", "fastapi_app:app", "--host", "0.0.0.0", "--port", "8000"]