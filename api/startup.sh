#! /bin/bash
python3 update_data_hourly.py cron.log &
gunicorn -b :5000 api:app --workers 3 --timeout 1000 &
wait