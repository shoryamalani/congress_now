import update_data_hourly
from sys import argv
cron_file = "cron.log"

if(len(argv)>1):
    cron_file = argv[1]

update_data_hourly.update_bills(cron_file)