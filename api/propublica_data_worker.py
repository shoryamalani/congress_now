#get data from propublica
#get key from environment variables
import os
import requests
import json
import sys
import dbs_worker
from dotenv import load_dotenv
import datetime
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
load_dotenv()
#get key from environment variables
api_key = os.environ.get('PROPUBLICA_API_KEY')
CURRENT_CONGRESS = '117'
#https://projects.propublica.org/api-docs/congress-api/members/#get-current-members

FULL_DAY = 86400
MAX_CALLS_PER_DAY = 5000

@sleep_and_retry
@limits(calls=MAX_CALLS_PER_DAY, period=FULL_DAY)
def send_request(url,headers,params={}):
    r = requests.get(url, headers=headers,params=params)
    if r.status_code == 200:
        return r.json()
    else:
        print(r.status_code)
        return None

def get_current_senate_members():
    url = f'https://api.propublica.org/congress/v1/{CURRENT_CONGRESS}/senate/members.json'
    headers = {'X-API-Key': api_key}
    return send_request(url,headers)

def get_current_member(member_id):
    url = f'https://api.propublica.org/congress/v1/members/{member_id}.json'
    headers = {'X-API-Key': api_key}
    return send_request(url,headers)

def get_member_vote_positions(member_id):
    url = f'https://api.propublica.org/congress/v1/members/{member_id}/votes.json'
    headers = {'X-API-Key': api_key}
    params = {'per_page': 250}
    return send_request(url,headers)

def get_current_house_members():
    url = f'https://api.propublica.org/congress/v1/{CURRENT_CONGRESS}/house/members.json'
    headers = {'X-API-Key': api_key}
    data = send_request(url,headers)
    if 'results' in data:
        return data
    return None

def get_current_house_and_senate_bills():
    # From house bills
    url = f'https://api.propublica.org/congress/v1/{CURRENT_CONGRESS}/house/bills/introduced.json'
    headers = {'X-API-Key': api_key}
    bills = send_request(url,headers)
    if 'results' in bills:
        bills = bills['results'][0]['bills']
    else:
        bills = [[]]

    # From senate bills
    url = f'https://api.propublica.org/congress/v1/{CURRENT_CONGRESS}/senate/bills/introduced.json'
    headers = {'X-API-Key': api_key}
    senate_bills = send_request(url,headers)
    if 'results' in senate_bills:
        senate_bills = senate_bills['results'][0]['bills']
    else:
        senate_bills = [[]]
    bills.extend(senate_bills)
    # r = requests.get(url, headers=headers)
    # if r.status_code == 200:
    #     bills.append(r.json()['results'][0])
    # else:
    #     bills.append([])
    return bills

def get_bill_data(bill_name,congress_num=CURRENT_CONGRESS):
    url = f'https://api.propublica.org/congress/v1/{congress_num}/bills/{bill_name}.json'
    headers = {'X-API-Key': api_key}
    params = {'slug': bill_name}
    data = send_request(url,headers,params)
    if 'results' in data:
        return data['results'][0]
    return None

    # r = requests.get(url, headers=headers)
    # if r.status_code == 200:
    #     return r.json()['results'][0]
    # else:
    #     return None
def add_propublica_data_to_db(data):
    # data = dbs_worker.get_all_bills(dbs_worker.set_up_connection())
    # data = get_current_house_and_senate_bills()[0]['bills'][0]
    threads = []
    all_data = []
    # final_bills = [[bill[3].split('_')[0],bill[3].split("_")[1]] for bill in data]
    final_bills = {bill[3].split('_')[0]:bill for bill in data}
    for bill,full_bill in final_bills.copy().items():
        if full_bill[1] != None:
            final_bills.pop(bill)

    bills_with_data = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        for bill,full_name in final_bills.items():
            threads.append(executor.submit(get_bill_data, bill))
        for task in as_completed(threads):
            data = task.result()
            bills_with_data.append((final_bills[data['bill_slug'].upper()][3],data))
        
    dbs_worker.update_bills_with_propublica(dbs_worker.set_up_connection(),bills_with_data)

            # dbs_worker.add_propublica_data(dbs_worker.set_up_connection(),task.result())
# add_propublica_data_to_db()

def search_bills_text(text):
    url = f'https://api.propublica.org/congress/v1/bills/search.json?query={text}'
    headers = {'X-API-Key': api_key}
    data = send_request(url,headers)
    if data == None:
        return None
    if 'results' in data:
        return data['results'][0]['bills']
    return None

def get_all_members_both_houses():
    data = get_current_house_members()['results'][0]['members']
    data.extend(get_current_senate_members()['results'][0]['members'])
    return data




if __name__ == "__main__":
    # add_propublica_data_to_db(dbs_worker.get_all_bills(dbs_worker.set_up_connection()))
    data = get_all_members_both_houses()
    print(data[0])
    dbs_worker.save_members_to_db(dbs_worker.set_up_connection(),[{"id":info["id"],"propublica_api":info,"congress_api":None,"congress_api_detailed":None,"house_or_senate":"house" if info['title'] == 'Representative' else 'senate'} for info in data])
    