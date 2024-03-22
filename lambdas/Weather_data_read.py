import json
import os
import urllib3
import boto3
from datetime import datetime


def lambda_handler(event, context):
    API_KEY = "70ecd2f04f324083bf4131452231703",
    Location = "EN6",
    url_new = "http://api.weatherapi.com/v1/forecast.json"
    url_complete = "http://api.weatherapi.com/v1/forecast.json?key=70ecd2f04f324083bf4131452231703&q=EN6&hours=1&aqi=no&alerts=no"
    days_api = 1
    # Set up http request maker thing
    # data = {"userId": None, "id": None, "title": None, "body": None}
    http = urllib3.PoolManager()
    try:
        r = http.request("GET", url_complete,
                         retries=urllib3.util.Retry(3))  # fields={"q":Location,"key":API_KEY,"days":days_api})
        data_frm_api = json.loads(r.data.decode("utf8").replace("'", '"'))
        # hours_arr_wind,hours_arr_heat=get_data_array(data_frm_api)
        # print(type(data_frm_api))

    except KeyError as e:
        print(f"Wrong format url {get_path}", e)
    except urllib3.exceptions.MaxRetryError as e:
        print(f"API unavailable at {get_path}", e)

    cilent = boto3.client('s3')

    filename = "weather_api" + str(datetime.now()) + ".json"

    cilent.put_object(
        Bucket="mohsinbucket1",
        Key="Toprocess/" + filename,
        Body=json.dumps(data_frm_api)
    )

