from dbs_scripts import write_and_read_to_database,execute_db
import dotenv
import os
import json
import psycopg2
import datetime
import pypika
import congress_data_api
import propublica_data_worker
from pypika import functions,Query
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
def is_docker():
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile(path) and any('docker' in line for line in open(path))
    )
def set_up_connection():
    # Path to .env file
    if not is_docker():
        dotenv_path = os.path.join(os.path.dirname(__file__), '../postgres/.env')
        # Load file from the path
        dotenv.load_dotenv(dotenv_path)
        # set up connection to postgres
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST_DEV'),
            database=os.environ.get('POSTGRES_DB'),
            user=os.environ.get('POSTGRES_USER'),
            password=os.environ.get('POSTGRES_PASSWORD')
        )
        return conn

    else:

        # dotenv_path = os.path.join(os.path.dirname(__file__), '/postgres/.env')
        # print(dotenv_path)
        print(os.listdir())
        # # Load file from the path
        # print(dotenv.load_dotenv(dotenv_path,verbose=True))
        # print(dotenv.dotenv_values(dotenv_path).items())
        dotenv.load_dotenv()

        # set up connection to postgres
        print(os.environ)
        print(os.environ.get('DB_HOST'))

        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST'),
            database=os.environ.get('POSTGRES_DB'),
            user=os.environ.get('POSTGRES_USER'),
            password=os.environ.get('POSTGRES_PASSWORD')
        )
        return conn

def write_bill(conn,bill_name,congress_data):
    congress_write = write_and_read_to_database.make_write_to_db('bills',(bill_name,congress_data),('bill_name','congress_api'))
    execute_db.execute_database_command(conn,congress_write)

def write_bills(conn,bills_data_tuple):
    final_bills = []
    for bill in bills_data_tuple:
        data = read_bill(conn,bill[0])
        print(data)
        if not data:
            final_bills.append(bill)
        elif data[4] == None:
            update_bill(conn,bill[0],{"congress_api":bill[1],"congress_api_detailed":bill[2]})
        
    if len(final_bills) == 0:
        return
    congress_write = write_and_read_to_database.make_write_to_db('bills',final_bills,('bill_name','congress_api,congress_api_detailed'))
    print(congress_write)
    [conn,cur] = execute_db.execute_database_command(conn,congress_write)
    conn.commit()



def read_bill(conn,bill_name):
    congress_read = write_and_read_to_database.get_from_where_db('bills','bill_name',bill_name)
    print(congress_read)
    data = execute_db.execute_database_command(conn,congress_read)
    if data[1].rowcount == 0:
        return False
    else:
        return data[1].fetchone()

def get_bill_name(bill_congress):
    bill_congress['bill_name'] = bill_congress['type'] + str(bill_congress['number']) + '_' + str(bill_congress['congress'])
    return bill_congress['bill_name']

def get_all_recent_bills(conn,tot):
    bills = get_all_bills(conn)
    final_bills = {}
    for bill in bills:
        if bill[0]['latestAction']['actionDate'] in final_bills:
            final_bills[bill[0]['latestAction']['actionDate']].append(bill)
        else:
            final_bills[bill[0]['latestAction']['actionDate']] = [bill]
    #get most recent 100 bills
    final_bills_list = []
    total = 0
    # final_bills.sort(key=lambda x: x[0]['latestAction']['actionDate'],reverse=True)
    date = datetime.datetime.now()
    while total < tot:
        if date.strftime('%Y-%m-%d') in final_bills:
            print(date.strftime('%Y-%m-%d'))
            if total > tot:
                break
            for bill in final_bills[date.strftime('%Y-%m-%d')]:
                final_bills_list.append(bill)
                total += 1
        date = date - datetime.timedelta(days=1)
    return congress_data_api.get_all_relevant_bill_info(final_bills_list)
    # return bills

def get_all_bills(conn):
    # congress_read = write_and_read_to_database.get_from_where_db('bills','bill_name','*')
    congress_read = pypika.Query.from_('bills').select('*')
    print(congress_read)
    data = execute_db.execute_database_command(conn,congress_read.get_sql())
    if data[1].rowcount == 0:
        return False
    else:
        return data[1].fetchall()
def update_bill(conn,bill_name,data):
    bills_table = pypika.Table('bills')
    a = pypika.Query.update(bills_table).set(bills_table.congress_api,data['congress_api']).set(bills_table.congress_api_detailed,data['congress_api_detailed']).where(bills_table.bill_name == bill_name)
    # pypika.Query.update('bills')._update_sql
    # congress_write = write_and_read_to_database.make_update_db('bills','congress_api',data['congress_api'],'bill_name',bill_name)
    # congress_write = write_and_read_to_database.make_write_to_db('bills',(bill_name,congress_data),('bill_name','congress_api'))
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    conn.commit()
def update_bills_with_propublica(conn,bills_data_tuple):
    bills_table = pypika.Table('bills')
    for bill in bills_data_tuple:
        a = pypika.Query.update(bills_table).set(bills_table.propublica_api,json.dumps(bill[1])).where(bills_table.bill_name == bill[0])
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()

def make_table_bills():
    s = """
CREATE TABLE public.bills (
	congress_api json NULL,
	propublica_api json NULL,
	uuid integer NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	bill_name text NULL,
	congress_api_detailed json NULL,
    display_data json NULL
);

    """
    conn = set_up_connection()
    [conn,cur] = execute_db.execute_database_command(conn,s)
    conn.commit()

def make_table_members():
    columns = [("id","varchar"),("congress_api","json"),("propublica_api","json"),("congress_api_detailed","json"),("propublica_api_detailed","json")]
    sql = pypika.Query.create_table('members').columns(
        pypika.Column('id',"VARCHAR",nullable=False),
        pypika.Column('congress_api',"JSON",nullable=True),
        pypika.Column('propublica_api',"JSON",nullable=True),
        pypika.Column('congress_api_detailed',"JSON",nullable=True),
        pypika.Column('last_updated',"TIMESTAMP",nullable=True),
        pypika.Column('house_or_senate',"varchar",nullable=True),
    ).unique('id').primary_key('id').if_not_exists()
    conn = set_up_connection()
    [conn,cur] = execute_db.execute_database_command(conn,sql.get_sql())
    conn.commit()

def get_recent_info():
    bills = congress_data_api.get_current_bills()
    congress_data_api.save_bills(bills)
    data = get_all_bills(set_up_connection())
    # print(data[0][4])
    congress_data_api.save_detailed_bills(data)
    data = get_all_bills(set_up_connection())
    propublica_data_worker.add_propublica_data_to_db(data)
    data = get_all_bills(set_up_connection())
    
def save_display_data():
    relevant_info = congress_data_api.get_all_relevant_bill_info(get_all_bills(set_up_connection()))
    for bill in relevant_info:
        add_display_info_to_bill(set_up_connection(),bill['slug'].replace("-","_"),json.dumps(bill))

def get_recent_info_propublica():
    bills = get_all_bills(set_up_connection())
    propublica_data_worker.add_propublica_data_to_db(bills)


def read_member(conn,member_id):
    congress_read = write_and_read_to_database.get_from_where_db('members','id',member_id)
    print(congress_read)
    data = execute_db.execute_database_command(conn,congress_read)
    if data[1].rowcount == 0:
        return False
    else:
        return data[1].fetchone()
    
def update_member(conn,member_id,data):
    members_table = pypika.Table('members')
    a = pypika.Query.update(members_table)
    if 'congress_api' in data:
        data['congress_api'] = json.dumps(data['congress_api'])
        a = a.set(members_table.congress_api,data['congress_api'])
    if 'propublica_api' in data:
        data['propublica_api'] = json.dumps(data['propublica_api'])
        a = a.set(members_table.propublica_api,data['propublica_api'])
    if 'congress_api_detailed' in data:
        data['congress_api_detailed'] = json.dumps(data['congress_api_detailed'])
        a = a.set(members_table.congress_api_detailed,data['congress_api_detailed'])
    a.set(members_table.last_updated,functions.CurTimestamp())
    a = a.where(members_table.id == member_id)
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    print(a.get_sql())
    conn.commit()
    

def insert_member(conn,id,data):
    members_table = pypika.Table('members')
    a = pypika.Query.into(members_table).columns('id','congress_api','propublica_api','congress_api_detailed','last_updated','house_or_senate').insert(id,json.dumps(data['congress_api']),json.dumps(data['propublica_api']),json.dumps(data['congress_api_detailed']),functions.CurTimestamp(),data['house_or_senate'])
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    conn.commit()

def save_members_to_db(conn,data):
    final_members = []
    for member in data:
        data = read_member(conn,member["id"])
        print(data)
        print(member)
        if not data:
            insert_member(conn,member["id"],member)
        elif any([data[2] != member["propublica_api"],data[1] != member["congress_api"],data[3] != member["congress_api_detailed"]]):
            update_member(conn,member["id"],member)
        
    # if len(final_members) == 0:
    return
    # congress_write = write_and_read_to_database.make_write_to_db('members',final_members,('id','propublica_api,propublica_api_detailed'))
    # update_member(conn,member["id"],member)
def get_all_members(conn):
    members_table = pypika.Table('members')
    a = pypika.Query.from_(members_table).select('*')
    print(a.get_sql())
    data = execute_db.execute_database_command(conn,a.get_sql())
    if data[1].rowcount == 0:
        return False
    else:
        return data[1].fetchall()

def get_all_detailed_info_for_all_members():
    data = get_all_members(set_up_connection())
    MAX_WORKERS = 1
    threads = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for member in data:
            if member[3] == None:
                # threads.append(executor.submit(get_detailed_info_for_member,member))
                threads.append(executor.submit(congress_data_api.get_current_member_detailed,member[0]))
                print(member[0])
            else:
                print('already have data')
        for thread in as_completed(threads):
            update_member(set_up_connection(),thread.result()['request']['bioguideId'].upper(),{'congress_api_detailed':thread.result()['member']})
        
    # for member in data:
    #     if member[4] == None:
    #         print(member[0])
    #         member_data = propublica_data_worker.get_member(member[0])
    #         member_data['house_or_senate'] = member[6]
    #         save_members_to_db(set_up_connection(),[member_data])
    #     if member[3] == None:
    #         print(member[0])
    #         member_data = congress_data_api.get_member(member[0])
    #         member_data['house_or_senate'] = member[6]
    #         save_members_to_db(set_up_connection(),[member_data])
        
    return data

def add_display_info_to_bill(conn,bill_id,bill_data):
    members_table = pypika.Table('bills')
    a = pypika.Query.update(members_table)
    a = a.set(members_table.display_data,bill_data)
    a = a.where(members_table.bill_name == bill_id)
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    print(a.get_sql())
    conn.commit()
if __name__ == "__main__":
    # get_recent_info()
    save_display_data()
    # get_recent_info_propublica()
    # print(make_table_members())
    # data = get_all_detailed_info_for_all_members()
    # print(get_all_members(set_up_connection()))
    # relevant_info = congress_data_api.get_all_relevant_bill_info(get_all_bills(set_up_connection()))
    # for bill in relevant_info:
    #     add_display_info_to_bill(set_up_connection(),bill['slug'].replace("-","_"),json.dumps(bill))