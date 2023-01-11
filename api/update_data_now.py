import update_data_hourly
from sys import argv
cron_file = "cron.log"

if(len(argv)>2):
    cron_file = argv[2]

update_data_hourly.update_bills(cron_file)