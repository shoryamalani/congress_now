import congress_data_api
import dbs_worker
import propublica_data_worker
import time


def set_up_hourly_data_server():
    conn = dbs_worker.set_up_connection()
    dbs_worker.create_sys_info_table(conn)
    



def update_bills():
    bills = propublica_data_worker.get_current_house_and_senate_bills()
    