import time
from flask import Flask, jsonify, request , redirect, url_for, request
import dbs_worker
import congress_data_api
import propublica_data_worker
import os
import json
app = Flask(__name__, static_folder='../build', static_url_path='/')


@app.errorhandler(404)
def not_found(e):
    return app.send_static_file('index.html')


@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/bill/<bill_slug>')
def bill_page(bill_slug):
    return app.send_static_file('index.html')

@app.route('/api/bill/<bill_slug>')
def bill_data(bill_slug):
    data = congress_data_api.get_bill_data(bill_slug)
    return jsonify(data)

@app.route('/api/all_bills')
def all_bills():
    if not os.path.isfile('current_all_bills.json'):
        data = get_current_data()
        return jsonify(data)
    else:
        with open('current_all_bills.json','r') as f:
            data = json.loads(f.read())
        return jsonify(data)

@app.route('/api/force_get_data',methods=['GET'])
def force_get_data():
    data = get_current_data()
    return jsonify(data)

@app.route('/api/bill_search_text',methods=['POST'])
def search_bills_text():
    print(request.json)
    bills = propublica_data_worker.search_bills_text(request.json['search_text'])
    # return jsonify(bills)
    dbs_worker.add_bills_with_propublica(dbs_worker.set_up_connection(),bills)
    bills_display = congress_data_api.get_all_relevant_bill_info_from_propublica(bills)
    return jsonify(bills_display)



def get_current_data():
    conn = dbs_worker.set_up_connection()
    data = dbs_worker.get_all_recent_bills(conn,50)
    # data = congress_data_api.get_all_relevant_bill_info(dbs_worker.get_all_bills(dbs_worker.set_up_connection()))
    with open('current_all_bills.json','w') as f:
        f.write(json.dumps(data))
    return data




if __name__ == "__main__":
    # app = Flask(__name__, static_folder='../src',static_url_path= '/')
    # data = dbs_worker.get_all_recent_bills(dbs_worker.set_up_connection())
    # for bill in data:
    #     print(bill["lastActionDate"])
    # print(congress_data_api.get_all_relevant_bill_info(dbs_worker.get_all_bills(dbs_worker.set_up_connection())))
    app.run(port=5000, debug=True)