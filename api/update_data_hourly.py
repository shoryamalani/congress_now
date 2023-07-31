import congress_data_api
import dbs_worker
import propublica_data_worker
import time
import datetime
import schedule
import time
import loguru
from sys import argv
import requests


def set_up_hourly_data_server():
    conn = dbs_worker.set_up_connection()
    dbs_worker.create_sys_info_table(conn)
    
def update_bills(log_file):
    loguru.logger.add(log_file, rotation="1 day", retention="7 days")
    loguru.logger.debug("Updated bills and members")
    conn = dbs_worker.set_up_connection()
    print("Updating bills")
    try:
        if not dbs_worker.check_if_bills_updated_in_last_12_hours(dbs_worker.set_up_connection()):
            bills = congress_data_api.get_current_bills_after(dbs_worker.get_last_bills_updated(conn))
            congress_data_api.save_bills(bills)
            dbs_worker.set_updated_bills(dbs_worker.set_up_connection())
        dbs_worker.update_bills(dbs_worker.set_up_connection(),25)
    except Exception as e:
        print("ERROR UPDATING BILLS")
        print(e)
    # if not dbs_worker.check_if_members_updated_in_last_24_hours(dbs_worker.set_up_connection()):
    try:
        to_update = []
        members = propublica_data_worker.get_all_members_both_houses()
        # print(members[0])
        members_current = dbs_worker.get_all_members_in_current_congress(dbs_worker.set_up_connection(),congress_data_api.get_current_congress())
        members_current_ids = [i[0] for i in members_current]
        for member in members:
            if member["id"] not in members_current_ids:
                to_update.append(member)
        i = 0
        while len(to_update) < 25:
            if i >= len(members_current):
                member = members_current[i]
                i+=1
                # print(member[4])
                if  datetime.datetime.now() - member[4]  >  datetime.timedelta(hours=24): 
                    to_update.append(member[2])
            else:
                i+=1
    except Exception as e:
        print("ERROR GETTING MEMBERS")
        print(e)
    conn = dbs_worker.set_up_connection()
    try:
        for i in to_update[:25]:
            print(i)
            dbs_worker.get_and_update_member_info(conn,i['id'] if 'id' in i else i['request']['bioguideId'].upper(),propublica_data=i)
        dbs_worker.rethink_bills(dbs_worker.set_up_connection())
    except Exception as e:
        print("ERROR UPDATING MEMBERS")
        print("ERROR Rethinking bills")
        print(e)
        # dbs_worker.get_recent_info(dbs_worker.set_up_connection())
    # congress_data_api.get_current_data() # gets new bill information
    requests.get('https://congressnow.shoryamalani.com/api/force_get_data')    

if __name__ == "__main__":
    # for i in range(24):
        # update_bills()
    try:
        update_bills(argv[1])
    except Exception as e:
        print(e)
    schedule.every().hour.do(update_bills,argv[1])
    while 1:
        try:
            schedule.run_pending()
        except:
            print("Error updating bills")
        time.sleep(1)


    