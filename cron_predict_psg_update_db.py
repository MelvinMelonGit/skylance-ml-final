#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# cron_predict_psg_update_db.py

# cron/CLI trigger—runs once and exits
'''
Set the cron to call in terminal:
# every hour
0 * * * * cd </path/to/your_project> && \
    DB_URL="mysql+pymysql://root:<password>@localhost:3306/skylance" \
    python cron_predict_psg_update_db.py >> logs/predict.log 2>&1
'''

import os
from passenger_predictor import get_engine_and_session, load_pipeline, fetch_features, predict_and_write_back

def main():
    engine, session = get_engine_and_session()
    pipeline = load_pipeline()

    df = fetch_features(engine)
    if df.empty:
        print("No new rows.")
        return

    n = predict_and_write_back(engine, session, pipeline, df)
    print(f"Updated {n} rows.")

if __name__ == "__main__":
    main()

