import os
import requests
import datetime
from dotenv import load_dotenv
import dbs_worker
import json
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from ratelimit import limits, sleep_and_retry
load_dotenv()
BASE_API_URL = 'https://api.congress.gov/v3'
API_KEY = os.environ.get('CONGRESS_API_KEY')

ONE_HOUR = 3600
MAX_CALLS_PER_HOUR = 800

@sleep_and_retry
@limits(calls=MAX_CALLS_PER_HOUR, period=ONE_HOUR)
def send_request(url,headers,params):
    r = requests.get(url, headers=headers,params=params)
    if r.status_code == 200:
        return r.json()
    else:
        print(r.status_code)
        return None

def get_current_bills_total(fromDate):
    # from bills
    from_date = fromDate.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f'{BASE_API_URL}/bill'
    headers = {'X-API-Key': API_KEY}
    params = {"limit":'250',"fromDateTime":from_date}
    bills = send_request(url,headers,params)
    return bills


def get_detailed_bill_info(bill_info):
    if type(bill_info) is not dict:
        bill_type = ""
        bill_number = ""
        for char in bill_info.split('_')[0]:
            if char.isdigit():
                bill_number += char
            else:
                bill_type += char
        url = BASE_API_URL + "/bill/" + bill_info.split('_')[1] + '/' + bill_type + "/" + bill_number
        print(url)
    else:
        url = bill_info['url']

    headers = {'X-API-Key': API_KEY}
    bill = send_request(url,headers,{})
    # get subjects
    try:
        if 'subjects'  in bill['bill']:
            url = bill['bill']['subjects']['url']
            headers = {'X-API-Key': API_KEY}
            bill['bill']['subjects']['data'] = send_request(url,headers,{})
        # get summaries
        if 'summaries' in bill['bill']:
            url = bill['bill']['summaries']['url']
            headers = {'X-API-Key': API_KEY}
            bill['bill']['summaries']['data'] = send_request(url,headers,{})
        # get related bills
        if 'relatedBills' in bill['bill']:
            url = bill['bill']['relatedBills']['url']
            headers = {'X-API-Key': API_KEY}
            bill['bill']['relatedBills']['data'] = send_request(url,headers,{})
        if 'text' in bill['bill']:
            url = bill['bill']['text']
            headers = {'X-API-Key': API_KEY}
            bill['bill']['text'] = send_request(url,headers,{})
        return bill
    except Exception as e:
        print(e)
        return None

def get_current_bills():
    data = get_current_bills_total(datetime.datetime.now() - datetime.timedelta(days=6))['bills']
    return data

def get_current_bills_after(start_date):
    data = get_current_bills_total(datetime.datetime.strptime(start_date,"%Y-%m-%d %H:%M:%S.%f"))['bills']
    return data

def save_detailed_bills_with_congress_start(starting_bills):
    # data = []
    # for bill in starting_bills:
    #     if bill[4]== None:
    #         data.append(bill[0])
    # print(len(data))
    bills = {}
    threads = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        for info in starting_bills:
            if info[0] == None:
                threads.append(executor.submit(get_detailed_bill_info,info))
            # data_threads = [{"congress_open":bill,"congress_detailed":executor.submit(get_detailed_bill_info,bill)} for bill in data[:1]]
            bills[dbs_worker.get_bill_name(info)] = [json.dumps(info)]
        print(bills)
        print(threads)
        wait(threads)
        for thread in threads:
            # bills.append((dbs_worker.get_bill_name(thread['congress_open']),json.dumps(thread['congress_open'],json.dumps(thread['congress_detailed'].result()))))
            d = thread.result()
            bills[dbs_worker.get_bill_name(d['bill'])].append(json.dumps(d))
    # for bill in data:
    #     bills.append((dbs_worker.get_bill_name(bill),json.dumps(bill)))
        print(bills)
        # bills_data = [(key,val[0],val[1] if len(val)==1) for key,val in bills.items()]
        bills_data = []
        for key,val in bills.items():
            if len(val) == 1:
                bills_data.append((key,val[0],None))
            else:
                bills_data.append((key,val[0],val[1]))
        
        conn = dbs_worker.set_up_connection()
        # dbs_worker.write_bill(conn,bills[0][0],bills[0][1])
        print(len(bills))
        json.dump(bills_data,open('bills.json','w+'))
        dbs_worker.write_bills(conn,bills_data)

def get_current_member_detailed(member_id):
    url = f'{BASE_API_URL}/member/{member_id}'
    headers = {'X-API-Key': API_KEY}
    member = send_request(url,headers,{'bioguideId':member_id,'format':'json'})
    print(member)
    return member

def get_current_house_members():
    url = f'{BASE_API_URL}/member'
    headers = {'X-API-Key': API_KEY}
    params = {"limit":'250'}
    members = send_request(url,headers,params)
    return members
def get_member_detailed_sponsored(member):
    url = BASE_API_URL + "/member/" + member + "/sponsored-legislation"
    headers = {'X-API-Key': API_KEY}
    params = {"limit":'250'}
    bills = send_request(url,headers,params)
    return bills

def get_member_detailed_cosponsored(member):
    url = BASE_API_URL + "/member/" + member + "/cosponsored-legislation"
    headers = {'X-API-Key': API_KEY}
    params = {"limit":'250'}
    bills = send_request(url,headers,params)
    return bills



def print_bills():
    data = dbs_worker.get_all_bills(dbs_worker.set_up_connection())

    for bill in data:
        if 'summaries' in bill[4]['bill']:
            print(bill[4]['bill']['summaries']['count'])

def get_bill_data(bill_slug):
    data = dbs_worker.get_bill(dbs_worker.set_up_connection(),bill_slug)
    display_info = get_all_relevant_bill_info([data])
    return display_info

def get_all_relevant_bill_info(bills):
    final_bills = []
    members = dbs_worker.get_all_members(dbs_worker.set_up_connection())
    member_data = {}

    for member in members:
        if member[2] != None and member[3] != None:
            member_data[member[0]] = {"pro":member[2],"con":member[3]}
        
    propublica_only = []
    no_member = []
    for bill in bills:
        
        # elif bill[0] == None:
        #     # final_bills.append(get_all_relevant_bill_info_from_propublica(bill[1],member_data))
        #     propublica_only.append(bill[1])
        if bill[1] == None:
            continue
        else:
            final_bill = {}
            bill_detailed = bill[4]['bill']
            bill_propublica = bill[1]
            final_bill['name'] = bill_propublica['short_title']
            # final_bill['url'] = 'https://www.congress.gov/bill/' + str(bill['congress']) + '/' + str(bill['type']) + '/' + str(bill['number'])
            final_bill['url'] = bill_propublica['congressdotgov_url']
            final_bill['congress_now_url'] = "http://congressnow.shoryamalani.com/bill/" + bill[3].upper()
            final_bill['govtrack'] = bill_propublica['govtrack_url']
            final_bill['sponsor'] = bill_detailed['sponsors'][0]['firstName'] + ' ' + bill_detailed['sponsors'][0]['lastName'] + ' (' + bill_detailed['sponsors'][0]['party'] + '-' + bill_detailed['sponsors'][0]['state'] + ')'
            final_bill['sponsorId'] = bill_detailed['sponsors'][0]['bioguideId'].upper()
            if final_bill['sponsorId'] in member_data:
                if "depiction" not in member_data[final_bill['sponsorId']]['con']:
                    final_bill['photo'] = ""
                else:
                    final_bill['photo'] = member_data[final_bill['sponsorId']]['con']['depiction']['imageUrl']
                final_bill['url'] = member_data[final_bill['sponsorId']]['pro']['url']
                final_bill['twitter_account'] = member_data[final_bill['sponsorId']]['pro']['twitter_account']
                final_bill['facebook_account'] = member_data[final_bill['sponsorId']]['pro']['facebook_account']
            else:
                final_bill['photo'] = None
                final_bill['url'] = None
                final_bill['twitter_account'] = None
                final_bill['facebook_account'] = None
            final_bill['sponsorParty'] = bill_detailed['sponsors'][0]['party']
            final_bill['sponsorState'] = bill_detailed['sponsors'][0]['state']
            final_bill['summary'] = bill_propublica['summary']
            final_bill['slug'] = bill_propublica['bill_id']

            # if 'data' in bill_detailed['summaries']:
            #     final_bill['summary'] =bill_detailed['summaries']['data']['summaries'][0]['text']
            final_bill['introducedDate'] = bill_detailed['introducedDate']
            final_bill['lastActionDate'] = bill_detailed['latestAction']['actionDate']
            final_bill['lastAction'] = bill_detailed['latestAction']['text']
            final_bill['votes'] = bill_propublica['votes']
            if 'lastVoteDate' in bill_detailed:
                final_bill['lastVoteDate'] = bill_detailed['lastVoteDate']
            else:
                final_bill['lastVoteDate'] = None
            if bill_propublica['cosponsors'] == 0:
                final_bill['cosponsors'] = 0
                final_bill['cosponsors_by_party'] = {"R":0,"D":0}
            else:
                final_bill['cosponsors'] = bill_detailed['cosponsors']['count']
                final_bill['cosponsors_by_party'] = bill_propublica['cosponsors_by_party']
                if "R" not in final_bill['cosponsors_by_party']:
                    final_bill['cosponsors_by_party']['R'] = 0
                if "D" not in final_bill['cosponsors_by_party']:
                    final_bill['cosponsors_by_party']['D'] = 0
            final_bill['committees'] = bill_propublica['committees']
            final_bill['primarySubject'] = bill_propublica['primary_subject']
            if 'relatedBills' in bill_detailed:
                final_bill['relatedBills'] = bill_detailed['relatedBills']
            else:
                final_bill['relatedBills'] = None
            final_bills.append(final_bill)
            dbs_worker.add_display_info_to_bill(dbs_worker.set_up_connection(),bill_propublica['bill_id'].replace("-","_").upper(),json.dumps(final_bill))
            json.dump(no_member,open("no_member.json","w+"))
    propublica_only = get_all_relevant_bill_info_from_propublica(propublica_only,member_data)
    for bill in propublica_only:
        final_bills.append(bill)
    return final_bills
def get_all_relevant_bill_info_from_propublica(bills,member_data=None):
    final_bills = []
    if member_data == None:
        members = dbs_worker.get_all_members(dbs_worker.set_up_connection())
        member_data = {}

        for member in members:
            if member[2] != None and member[3] != None:
                member_data[member[0]] = {"pro":member[2],"con":member[3]}
        

    for bill in bills:
        final_bill = {}
        bill_propublica = bill
        print(bill)
        if bill_propublica == None:
            continue
        # final_bill['name'] = bill_propublica['bill_id'].replace("-"," ").upper()
        final_bill['name'] = bill_propublica['short_title']
        final_bill['url'] = bill_propublica['congressdotgov_url']
        final_bill['congress_now_url'] = "http://congressnow.shoryamalani.com" + "/bill/" + bill['bill_id'].replace("-","_").upper()
        if 'govtrack_url' in bill_propublica:
            final_bill['govtrack'] = bill_propublica['govtrack_url']
        else:
            final_bill['govtrack'] = "https://www.govtrack.us/congress/bills/" + str(bill_propublica['congress']) + "/" + bill_propublica['bill_id'].replace("-","").upper()
        if 'sponsor_name' in bill_propublica:
            final_bill['sponsor'] = bill_propublica['sponsor_title'] + ' ' + bill_propublica['sponsor_name'] + ' (' + bill_propublica['sponsor_party'] + '-' + bill_propublica['sponsor_state'] + ')'
        else:
            final_bill['sponsor'] = bill_propublica['sponsor_title'] + ' ' + bill_propublica['sponsor'] + ' (' + bill_propublica['sponsor_party'] + '-' + bill_propublica['sponsor_state'] + ')'
        final_bill['sponsorId'] = bill_propublica['sponsor_id']
        if final_bill['sponsorId'] in member_data:
            final_bill['photo'] = member_data[final_bill['sponsorId']]['con']['depiction']['imageUrl']
            final_bill['url'] = member_data[final_bill['sponsorId']]['pro']['url']
        final_bill['sponsorParty'] = bill_propublica['sponsor_party']
        final_bill['sponsorState'] = bill_propublica['sponsor_state']
        final_bill['summary'] = bill_propublica['summary']
        final_bill['slug'] = bill_propublica['bill_id']

        # if 'data' in bill_detailed['summaries']:
        #     final_bill['summary'] =bill_detailed['summaries']['data']['summaries'][0]['text']
        final_bill['introducedDate'] = bill_propublica['introduced_date']
        final_bill['lastActionDate'] = bill_propublica['latest_major_action_date']
        final_bill['lastAction'] = bill_propublica['latest_major_action']
        final_bill['votes'] = []
        # if bill_propublica['votes'] != []:
        #     final_bill['lastVoteDate'] = bill_propublica['votes'][-1]
        # else:
        #     final_bill['lastVoteDate'] = None
        if bill_propublica['cosponsors'] == 0:
            final_bill['cosponsors'] = 0
            final_bill['cosponsors_by_party'] = {"R":0,"D":0}
        else:
            final_bill['cosponsors'] = bill_propublica['cosponsors']
            final_bill['cosponsors_by_party'] = bill_propublica['cosponsors_by_party']
            if "R" not in final_bill['cosponsors_by_party']:
                final_bill['cosponsors_by_party']['R'] = 0
            if "D" not in final_bill['cosponsors_by_party']:
                final_bill['cosponsors_by_party']['D'] = 0
        final_bill['committees'] = bill_propublica['committees']
        final_bill['primarySubject'] = bill_propublica['primary_subject']
        # if 'relatedBills' in bill_detailed:
        #     final_bill['relatedBills'] = bill_detailed['relatedBills']
        # else:
        final_bill['relatedBills'] = None
        final_bills.append(final_bill)
        dbs_worker.add_display_info_to_bill(dbs_worker.set_up_connection(),bill_propublica['bill_id'].replace("-","_").upper(),json.dumps(final_bill))
    return final_bills

def save_bills(bills):
    conn = dbs_worker.set_up_connection()
    final_bills = [(dbs_worker.get_bill_name(bill),bill,None,True) for bill in bills]
    dbs_worker.write_bills_for_later_from_cong(conn,final_bills)
    # for bill in bills:
    #     print(bill)
    #     dbs_worker.write_bill(conn,dbs_worker.get_bill_name(bill),bill)

# data = dbs_worker.get_all_bills(dbs_worker.set_up_connection())

def get_current_congress():
    #get congress number
    url = BASE_API_URL + "/congress"
    headers = {'X-API-Key': API_KEY}
    data = send_request(url, headers, {})
    num = ""
    for char in data['congresses'][0]["name"]:
        if char.isdigit():
            num += char
    return int(num)

def get_current_data():
    conn = dbs_worker.set_up_connection()
    data = dbs_worker.get_all_recent_bills(conn,50)
    # data = congress_data_api.get_all_relevant_bill_info(dbs_worker.get_all_bills(dbs_worker.set_up_connection()))
    with open('current_all_bills.json','w') as f:
        print(data)
        f.write(json.dumps(data))
    return data

if __name__ == "__main__":
    # data = get_current_house_members()
    # print(data)
    # print(len(data["members"]))
    print(get_current_congress())
