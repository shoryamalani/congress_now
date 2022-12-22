import congress_data_api
import dbs_worker
import propublica_data_worker
import time


def set_up_hourly_data_server():
    conn = dbs_worker.set_up_connection()
    dbs_worker.create_sys_info_table(conn)
    



def update_bills():
    if not dbs_worker.check_if_bills_updated_in_last_12_hours(dbs_worker.set_up_connection()):
        print("Updating bills")
        dbs_worker.get_recent_info(dbs_worker.set_up_connection())
    