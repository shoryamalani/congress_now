import time
from flask import Flask, jsonify, request
import dbs_worker
import congress_data_api
import propublica_data_worker
app = Flask(__name__, static_folder='../build', static_url_path='/')


@app.errorhandler(404)
def not_found(e):
    return app.send_static_file('index.html')


@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/api/time')
def get_current_time():
    return {'time': time.time()}

@app.route('/api/all_bills')
def all_bills():
    conn = dbs_worker.set_up_connection()
    data = dbs_worker.get_all_recent_bills(conn,50)
    # data = congress_data_api.get_all_relevant_bill_info(dbs_worker.get_all_bills(dbs_worker.set_up_connection()))
    return jsonify(data)
@app.route('/api/bill_search_text')
def search_bills_text():
    propublica_data_worker.search_bills_text(request.json['search_text'])






if __name__ == "__main__":
    app = Flask(__name__, static_folder='../src',static_url_path= '/')
    # data = dbs_worker.get_all_recent_bills(dbs_worker.set_up_connection())
    # for bill in data:
    #     print(bill["lastActionDate"])
    # print(congress_data_api.get_all_relevant_bill_info(dbs_worker.get_all_bills(dbs_worker.set_up_connection())))
    app.run()