#!/usr/bin/python3
import json
import urllib.parse
from botocore.vendored import requests
import boto3


def write_to_s3(st):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='raw_sas_cph_lon')
    s3.Object('raw_sas_cph_lon', 'cph_lon_20181201.json').put(Body=st)


def parse_config():
    origin = 'CPH'
    destination = 'LON'
    date = '20181201'
    print('Running with origin=' + origin + ',destination=' + destination + ', date=' + date)
    return {
        "displayType": "CALENDAR",
        "channel": "web",
        "bookingFlow": "REVENUE",
        "yth": "1",
        "outDate": date,
        "inDate": date,
        "from": origin,
        "to": destination,
        "pos": "dk"
        }


def get_flight_dates_union(raw_json):
    i = raw_json['inboundLowestFares']
    o = raw_json['outboundLowestFares']
    dates = set(i.keys()).union(set(o.keys()))
    return sorted(list(dates))


def fetch_json(conf):
    url = "https://api.flysas.com/offers/flights?{}".format(urllib.parse.urlencode(conf))
    return requests.get(url).json()


def get_flight_price(json, date):
    date_object = json.get(date, {'n': 'null'})
    return str(date_object.get('totalPrice', 'Null')) + " " + str(date_object.get('currency', ''))


def get_lowest_fares(json, sorted_dates):
    return [[k, get_flight_price(json['outboundLowestFares'], k), get_flight_price(json['inboundLowestFares'], k)]
            for k in sorted_dates]


def print_pretty_table(headers, data):
    t = PrettyTable(headers)
    for l in data:
        t.add_row(l)
    print(t)


def write_to_file(data):
    with open(sys.argv[1] + '-' + sys.argv[2], "w") as f:
        wr = csv.writer(f)
        wr.writerows(data)


def main():
    conf = parse_config()
    raw_json = fetch_json(conf)
    write_to_s3(raw_json)
    return raw_json
#    dates = get_flight_dates_union(raw_json)
#    headers = ['Date', 'outbound', 'inbound']
#    data = get_lowest_fares(raw_json, dates)
#    write_to_file(data)
#    print_pretty_table(headers, data)


def fetch_raw(event, context):
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": main() 
    }

    response = {
        "statusCode": 200,
        "body": json.dump(body)
    ]

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """

