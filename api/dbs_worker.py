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
    return bill_congress['bill_name'].upper()

def get_all_recent_bills(conn,tot):
    bills = get_all_bills(conn)
    final_bills = {}
    for bill in bills:
        if bill[0] == None:
            continue
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
    a = pypika.Query.update(bills_table).set(bills_table.congress_api,data['congress_api']).set(bills_table.congress_api_detailed,data['congress_api_detailed']).where(bills_table.bill_name == bill_name.upper())
    # pypika.Query.update('bills')._update_sql
    # congress_write = write_and_read_to_database.make_update_db('bills','congress_api',data['congress_api'],'bill_name',bill_name)
    # congress_write = write_and_read_to_database.make_write_to_db('bills',(bill_name,congress_data),('bill_name','congress_api'))
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    conn.commit()
def update_bills_with_propublica(conn,bills_data_tuple):
    bills_table = pypika.Table('bills')
    for bill in bills_data_tuple:
        a = pypika.Query.update(bills_table).set(bills_table.propublica_api,json.dumps(bill[1])).where(bills_table.bill_name == bill[0].upper())
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
    display_data json NULL,
    to_update boolean NULL
);

    """
    conn = set_up_connection()
    [conn,cur] = execute_db.execute_database_command(conn,s)
    conn.commit()
def add_update_to_bills(conn):
    # a = "ALTER TABLE bills ADD to_update boolean NULL;"
    # [conn,cur] = execute_db.execute_database_command(conn,a)
    # conn.commit()
    # a = pypika.Query.update('bills').set('to_update',False).where('to_update' == None)
    a = pypika.Query.update('bills').set('to_update',False)
    bills = pypika.Table('bills')

    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    conn.commit()
    all_bills = get_all_bills(conn)
    for bill in all_bills:
        set_to_update = False
        for key in bill:
            if key == None:
                set_to_update = True
        if set_to_update == True:
            print(bill[3])
        # a = pypika.Query.update('bills').set('to_update',set_to_update).where('bill_name' == bill[3])
        a = pypika.Query.update(bills).set(bills.to_update,set_to_update)
        a = a.where(bills.bill_name == bill[3])
        
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
        if bill[3].upper() != bill[3]:
            a = pypika.Query.update(bills).set(bills.bill_name,bill[3].upper())
            a = a.where(bills.bill_name == bill[3])
            [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
            conn.commit()

def remove_bill_repeats(conn):
    #remove bills with the same name
    bills = get_all_bills(conn)
    i = 0
    while i < len(bills):
        j = i + 1
        while j < len(bills):
            if bills[i][3] == bills[j][3]:
                print(bills[i][3])
                delete_bill(conn,bills[i][2])
            j += 1
        i += 1

def delete_bill(conn,uuid):
    # a = "DELETE FROM bills WHERE uuid = " + str(uuid) + ";"
    # [conn,cur] = execute_db.execute_database_command(conn,a)
    # conn.commit()
    bills = pypika.Table('bills')
    a = pypika.Query.from_(bills).delete().where(bills.uuid == uuid)
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
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
        pypika.Column('congress_num',"int",nullable=True)
    ).unique('id').primary_key('id').if_not_exists()
    conn = set_up_connection()
    [conn,cur] = execute_db.execute_database_command(conn,sql.get_sql())
    conn.commit()

def add_congress_number_to_members_table(conn):
    a = "ALTER TABLE public.members ADD congress_num int NULL;"
    [conn,cur] = execute_db.execute_database_command(conn,a)
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
    if 'congress_num' in data:
        a = a.set(members_table.congress_num,data['congress_num'])
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

def update_all_member_congress_num(conn):
    members = get_all_members(conn)
    for member in members:
        if member[6] == None:
            update_member(conn,member[0],{'congress_num':member[3]['terms'][-1]['congress']})

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
    a = a.where(members_table.bill_name == bill_id.upper())
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    print(a.get_sql())
    conn.commit()

def check_if_bill_exists(conn,bill_id):
    members_table = pypika.Table('bills')
    a = pypika.Query.from_(members_table).select('*').where(members_table.bill_name == bill_id.upper())
    print(a.get_sql())
    data = execute_db.execute_database_command(conn,a.get_sql())
    if data[1].rowcount == 0:
        return False
    else:
        return True

def add_bills_with_propublica(conn,bills):
    bills_table = pypika.Table('bills')
    for bill in bills:
        #check if bill exists
        if check_if_bill_exists(conn,bill['bill_id'].replace("-","_").upper()):
            continue
        a = pypika.Query.into(bills_table).columns('bill_name','propublica_api').insert(bill['bill_id'].replace("-","_").upper(),json.dumps(bill))
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()

def check_if_table_exists(conn,table_name):
    members_table = pypika.Table(table_name)
    a = pypika.Query.from_(members_table).select('*')
    data = execute_db.execute_database_command(conn,a.get_sql())
    if data[1].rowcount == 0:
        return False
    else:
        return True

def create_sys_info_table(conn):
    members_table = pypika.Table('sys_info')
    if check_if_table_exists(conn,'sys_info'):
        return
    a = pypika.Query.create_table(members_table).if_not_exists().columns(
        pypika.Column('id',pypika.Int()),
        pypika.Column('last_updated',pypika.DateTime()),
        pypika.Column('data',pypika.JSON())
    )
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    conn.commit()
    a = pypika.Query.into(members_table).columns('id','last_updated','data').insert(1,datetime.datetime.now(),json.dumps({'bills_recent':str(datetime.datetime.now()-datetime.timedelta(days=1)),'members_recent':str(datetime.datetime.now()-datetime.timedelta(days=1))}))


def check_if_bills_updated_in_last_12_hours(conn):
    if check_if_table_exists(conn,'sys_info'):
        a = pypika.Query.from_('sys_info').where('id = 1')
        data = execute_db.execute_database_command(conn,a.get_sql())
        if data[1].rowcount == 0:
            return False
        else:
            data = data[1].fetchall()
            data = json.loads(data[0][2])
            if datetime.datetime.now() - datetime.datetime.strptime(data['bills_recent'],'%Y-%m-%d %H:%M:%S.%f') > datetime.timedelta(hours=12):
                return False
            else:
                return True


def get_all_bills_to_update(conn):
    members_table = pypika.Table('bills')
    a = pypika.Query.from_(members_table).select('*').where(members_table.to_update == True)
    data = execute_db.execute_database_command(conn,a.get_sql())
    if data[1].rowcount == 0:
        return False
    else:
        return data[1].fetchall()


def update_bills(conn,num):
    bills = get_all_bills_to_update(conn)[:num]
    bills_table = pypika.Table('bills')
    bill_display_data = []
    for bill in bills:
        bill_data_congress_detailed = congress_data_api.get_detailed_bill_info(bill[3])
        a = pypika.Query.update(bills_table).set(bills_table.congress_api_detailed,json.dumps(bill_data_congress_detailed))
        a = a.where(bills_table.bill_name == bill[3])
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
        bill_data_propublica = propublica_data_worker.get_bill_data(bill[3].split("_")[0],bill[3].split("_")[1])
        a = pypika.Query.update(bills_table).set(bills_table.propublica_api,json.dumps(bill_data_propublica))
        a = a.where(bills_table.bill_name == bill[3])
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
        bill_display_data.append(bill_data_propublica)
        a = pypika.Query.update(bills_table).set(bills_table.to_update,False)
        a = a.where(bills_table.bill_name == bill[3])
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
        print(bill[3])
    congress_data_api.get_all_relevant_bill_info_from_propublica(bill_display_data)

        
        # add_display_info_to_bill(conn,bill[0],json.dumps(bill_data))
        # update_bill_to_update(conn,bill[0])
def write_bills_for_later_from_cong(conn,bills):
    bills_table = pypika.Table('bills')
    for bill in bills:
        if check_if_bill_exists(conn,get_bill_name(bill)):
            a = pypika.Query.update(bills_table).set(bills_table.to_update,True)
            a = a.where(bills_table.bill_name == bill)
            [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
            a = pypika.Query.update(bills_table).set(bills_table.congress_api,json.dumps(bill))
            a = a.where(bills_table.bill_name == bill)
            [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
            conn.commit()
        else:
            a = pypika.Query.into(bills_table).columns('bill_name','congress_api','to_update').insert(get_bill_name(bill),json.dumps(bill),True)
            [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
            conn.commit()

if __name__ == "__main__":
    # pass
    # add_update_to_bills(set_up_connection())
    update_bills(set_up_connection(),50)
    # remove_bill_repeats(set_up_connection())
    # get_recent_info()
    # save_display_data()
    # add_congress_number_to_members_table(set_up_connection())
    # update_all_member_congress_num(set_up_connection())
    # get_recent_info_propublica()
    # print(make_table_members())
    # data = get_all_detailed_info_for_all_members()
    # print(get_all_members(set_up_connection()))
    # relevant_info = congress_data_api.get_all_relevant_bill_info(get_all_bills(set_up_connection()))
    # for bill in relevant_info:
    #     add_display_info_to_bill(set_up_connection(),bill['slug'].replace("-","_"),json.dumps(bill))