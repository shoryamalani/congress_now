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

def get_all_bills_that_have_been_updated(conn):
    bills = pypika.Table('bills')
    query = Query.from_(bills).select('*').where(bills.to_update==False)
    data = execute_db.execute_database_command(conn,query.get_sql())
    return data[1].fetchall()

def get_all_recent_bills(conn,tot):
    # bills = get_all_bills(conn)
    bills = get_all_bills_that_have_been_updated(conn)[:tot]
    final_bills = {}
    for bill in bills:
        if bill[1]['latest_major_action_date'] in final_bills:
            final_bills[bill[1]['latest_major_action_date']].append(bill)
        else:
            final_bills[bill[1]['latest_major_action_date']] = [bill]
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
    rethink_bills(conn)
    
def rethink_bills(conn):
    bills = pypika.Table('bills')
    all_bills = get_all_bills(conn)
    for bill in all_bills:
        set_to_update = False
        for key in bill[1:5]:
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
        pypika.Column('congress_num',"int",nullable=True),
        pypika.Column('propublica_api_vote_positions',"JSON",nullable=True),
        pypika.Column('congress_api_sponsored_and_cosponsored',"JSON",nullable=True),
    ).unique('id').primary_key('id').if_not_exists()
    conn = set_up_connection()
    [conn,cur] = execute_db.execute_database_command(conn,sql.get_sql())
    conn.commit()

def get_and_update_member_info(conn,member_id,propublica_data=None):
    #Congress API update
    
    congress_member = congress_data_api.get_current_member_detailed(member_id)['member']

    congress_member_sponsored = congress_data_api.get_member_detailed_sponsored(member_id)
    if congress_member_sponsored == None:
        congress_member_sponsored = []
    else:
        congress_member_sponsored = congress_member_sponsored['sponsoredLegislation']
    congress_member_cosponsored = congress_data_api.get_member_detailed_cosponsored(member_id)
    if congress_member_cosponsored == None:
        congress_member_cosponsored = []
    else:
        congress_member_cosponsored = congress_member_cosponsored['cosponsoredLegislation']
    sponsored_and_cosponsored = {"sponsored":congress_member_sponsored,"cosponsored":congress_member_cosponsored}
    #ProPublica API update
    if propublica_data == None:
        propublica_member = propublica_data_worker.get_current_member(member_id)['results'][0]
    else:
        propublica_member = propublica_data
    propublica_member_vote_positions = propublica_data_worker.get_member_vote_positions(member_id)['results'][0]
    #jsonify all
    congress_member = json.dumps(congress_member)
    propublica_member = json.dumps(propublica_member)
    propublica_member_vote_positions = json.dumps(propublica_member_vote_positions)
    sponsored_and_cosponsored = json.dumps(sponsored_and_cosponsored)

    #Update database
    members = pypika.Table('members')
    # check if member exists
    if member_exists(conn,member_id) == False:
        a = pypika.Query.into(members).columns(members.id,members.congress_api,members.propublica_api,members.congress_api_detailed,members.propublica_api_vote_positions,members.congress_api_sponsored_and_cosponsored,members.last_updated,members.house_or_senate,members.congress_num).insert(member_id,congress_member,propublica_member,congress_member,propublica_member_vote_positions,sponsored_and_cosponsored,functions.Now(),json.loads(congress_member)['terms'][-1]['chamber'],json.loads(congress_member)['terms'][-1]['congress'])
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
    else:
        a = pypika.Query.update(members).set(members.congress_api,congress_member)
        a = a.set(members.propublica_api,propublica_member)
        a = a.set(members.congress_api_detailed,congress_member)
        a = a.set(members.propublica_api_vote_positions,propublica_member_vote_positions)
        a = a.set(members.congress_api_sponsored_and_cosponsored,sponsored_and_cosponsored)
        a = a.set(members.last_updated,functions.Now())
        a = a.set(members.house_or_senate,json.loads(congress_member)['terms'][-1]['chamber'])
        a = a.set(members.congress_num,json.loads(congress_member)['terms'][-1]['congress'])
        a = a.where(members.id == member_id)
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()

def member_exists(conn,member_id):
    members = pypika.Table('members')
    a = pypika.Query.from_(members).select(members.id).where(members.id == member_id)
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    data = cur.fetchall()
    if len(data) == 0:
        return False
    else:
        return True
def add_congress_number_to_members_table(conn):
    a = "ALTER TABLE public.members ADD congress_num int NULL;"
    [conn,cur] = execute_db.execute_database_command(conn,a)
    conn.commit()


# def get_recent_info():
    
    # data = get_all_bills(set_up_connection())
    # # print(data[0][4])
    # congress_data_api.save_detailed_bills(data)
    # data = get_all_bills(set_up_connection())
    # propublica_data_worker.add_propublica_data_to_db(data)
    # data = get_all_bills(set_up_connection())
    
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
        return []
    else:
        return data[1].fetchall()

def update_all_member_congress_num(conn):
    members = get_all_members(conn)
    for member in members:
        if member[6] == None:
            update_member(conn,member[0],{'congress_num':member[3]['terms'][-1]['congress']})

def get_all_detailed_info_for_all_members():
    data = get_all_members(set_up_connection())
    MAX_WORKERS = 10
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

def get_bill(conn,bill_id):
    members_table = pypika.Table('bills')
    a = pypika.Query.from_(members_table).select('*').where(members_table.bill_name == bill_id.upper())
    print(a.get_sql())
    data = execute_db.execute_database_command(conn,a.get_sql())
    if data[1].rowcount == 0:
        return None
    else:
        return data[1].fetchone()
def add_bills_with_propublica(conn,bills):
    bills_table = pypika.Table('bills')
    for bill in bills:
        #check if bill exists
        if check_if_bill_exists(conn,bill['bill_id'].replace("-","_").upper()):
            continue
        a = pypika.Query.into(bills_table).columns('bill_name','propublica_api','to_update').insert(bill['bill_id'].replace("-","_").upper(),json.dumps(bill),True)
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
    a = pypika.Query.create_table('sys_info').if_not_exists().columns(
        pypika.Column('id','int',nullable=False),
        pypika.Column('last_updated','timestamp',nullable=True),
        pypika.Column('data',"json",nullable=True)
    )
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    conn.commit()
    a = pypika.Query.into(pypika.Table('sys_info')).columns('id','last_updated','data').insert(1,functions.CurTimestamp(),json.dumps({'bills_recent':str(datetime.datetime.now()-datetime.timedelta(days=1)),'members_recent':str(datetime.datetime.now()-datetime.timedelta(days=1))}))
    [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
    conn.commit()


def check_if_bills_updated_in_last_12_hours(conn):
    if check_if_table_exists(conn,'sys_info'):
        sys_info_table = pypika.Table('sys_info')
        a = pypika.Query.from_(sys_info_table).select('*').where(sys_info_table.id == 1)
        data = execute_db.execute_database_command(conn,a.get_sql())
        if data[1].rowcount == 0:
            return False
        else:
            data = data[1].fetchall()[0][2]
            # data = json.loads(data[0][2])
            if datetime.datetime.now() - datetime.datetime.strptime(data['bills_recent'],'%Y-%m-%d %H:%M:%S.%f') > datetime.timedelta(hours=12):
                return False
            else:
                return True

def get_last_bills_updated(conn):
    sys_info_table = pypika.Table('sys_info')
    if check_if_table_exists(conn,'sys_info'):
        # a = pypika.Query.('sys_info').where(sys_info_table.id, 1)
        a = sys_info_table.select('*').where(sys_info_table.id == 1)
        data = execute_db.execute_database_command(conn,a.get_sql())
        if data[1].rowcount == 0:
            return False
        else:
            data = data[1].fetchall()
            data = data[0][2]
            return data['bills_recent']

def check_if_members_updated_in_last_24_hours(conn):
    if check_if_table_exists(conn,'sys_info'):
        a = pypika.Query.from_('sys_info').where('id = 1')
        data = execute_db.execute_database_command(conn,a.get_sql())
        if data[1].rowcount == 0:
            return False
        else:
            data = data[1].fetchall()
            data = json.loads(data[0][2])
            if datetime.datetime.now() - datetime.datetime.strptime(data['members_recent'],'%Y-%m-%d %H:%M:%S.%f') > datetime.timedelta(hours=24):
                return False
            else:
                return True
def set_updated_bills(conn):
    if check_if_table_exists(conn,'sys_info'):
        a = pypika.Query.update('sys_info').set('last_updated',datetime.datetime.now()).set('data',json.dumps({'bills_recent':str(datetime.datetime.now()),'members_recent':get_last_bills_updated(conn)}))
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()

def set_updated_bills_with_time(conn,time):
    if check_if_table_exists(conn,'sys_info'):
        a = pypika.Query.update('sys_info').set('last_updated',datetime.datetime.now()).set('data',json.dumps({'bills_recent':str(time),'members_recent':get_last_bills_updated(conn)}))
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
def get_all_bills_to_update(conn):
    members_table = pypika.Table('bills')
    a = pypika.Query.from_(members_table).select('*').where(members_table.to_update == True)
    data = execute_db.execute_database_command(conn,a.get_sql())
    if data[1].rowcount == 0:
        return False
    else:
        return data[1].fetchall()


def update_bills(conn,num):
    if get_all_bills_to_update(conn) == False:
        return
    all_bills = get_all_bills_to_update(conn)
    final_bills = {}
    for bill in all_bills:
        if bill[0]['latestAction']['actionDate'] in final_bills:
            final_bills[bill[0]['latestAction']['actionDate']].append(bill)
        else:
            final_bills[bill[0]['latestAction']['actionDate']] = [bill] 


    bills = []
    sorted_keys = sorted(final_bills.keys())
    for key in sorted_keys:
        if len(bills) >= num:
            break
        for bill in final_bills[key]:
            if len(bills) >= num:
                break
            bills.append(bill)

    bills_table = pypika.Table('bills')
    bill_display_data = []
    for bill in bills:
        bill_data_congress_detailed = congress_data_api.get_detailed_bill_info(bill[3])
        a = pypika.Query.update(bills_table).set(bills_table.congress_api_detailed,json.dumps(bill_data_congress_detailed))
        a = a.where(bills_table.bill_name == bill[3])
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
        bill_data_propublica = propublica_data_worker.get_bill_data(bill[3].split("_")[0],bill[3].split("_")[1])
        if bill_data_propublica == None:
            pass
        a = pypika.Query.update(bills_table).set(bills_table.propublica_api,json.dumps(bill_data_propublica))
        a = a.where(bills_table.bill_name == bill[3])
        [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
        conn.commit()
        if bill_data_propublica != None and bill_data_congress_detailed != None:
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
        bills_name = get_bill_name(bill[1])
        if check_if_bill_exists(conn,bills_name):
            a = pypika.Query.update(bills_table).set(bills_table.to_update,True)
            a = a.where(bills_table.bill_name == bills_name)
            [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
            a = pypika.Query.update(bills_table).set(bills_table.congress_api,json.dumps(bill[1]))
            a = a.where(bills_table.bill_name == bills_name)
            [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
            conn.commit()
        else:
            a = pypika.Query.into(bills_table).columns('bill_name','congress_api','to_update').insert(bills_name,json.dumps(bill[1]),True)
            [conn,cur] = execute_db.execute_database_command(conn,a.get_sql())
            conn.commit()

def get_all_members_in_current_congress(conn,congress_num):
    members_table = pypika.Table('members')
    a = pypika.Query.from_(members_table).select('*').where(members_table.congress_num == congress_num)
    data = execute_db.execute_database_command(conn,a.get_sql())
    if data[1].rowcount == 0:
        return []
    else:
        return data[1].fetchall()

if __name__ == "__main__":
    # pass
    create_sys_info_table(set_up_connection())
    make_table_bills()
    make_table_members()
    # add_update_to_bills(set_up_connection())  
    # update_bills(set_up_connection(),50)
    # make_table_members()
    # members = propublica_data_worker.get_all_members_both_houses()
    # get_and_update_member_info(set_up_connection(),members[0]["id"])
    # create_sys_info_table(set_up_connection())
    # rethink_bills(set_up_connection())
    # set_updated_bills_with_time(set_up_connection(),(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.%f"))
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