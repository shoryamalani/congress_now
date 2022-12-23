import api
import requests
import time
import congress_data_api
def test_get_current_bills():
    start_time = time.time()
    bills = requests.get('http://127.0.0.1:5000/api/all_bills').json()
    end_time = time.time()
    print(len(bills))
    assert len(bills) > 0
    print("test_get_current_bills took {} seconds".format(end_time - start_time))

def test_get_relevant_bill_info(slug):
    start_time = time.time()
    data = congress_data_api.get_bill_data(slug)
    end_time = time.time()
    print(data)
    print("test_get_relevant_bill_info took {} seconds".format(end_time - start_time))

if __name__ == "__main__":
    # test_get_current_bills()
    test_get_relevant_bill_info("S4120_117")