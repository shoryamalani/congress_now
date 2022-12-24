#! /bin/bash
# python3 update_data_hourly.py cron.log &
python3 update_data_now.py cron.log &
gunicorn -b 0.0.0.0:5000 api:app --workers 1 --timeout 15  &
wait