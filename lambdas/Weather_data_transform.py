import json
import os
import urllib3
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd


def get_data_array(forecast_data):
    hours_array_wind = []  # Create an array that will store each day as an array.
    hours_array_heat = []
    for i, v in enumerate(forecast_data['forecast']['forecastday']):
        hour_list = forecast_data['forecast']['forecastday'][i]['hour']  # Each day is here
        for k in hour_list:
            # print(k)
            hours_array_wind.extend([[k['time'], k['wind_kph'], k['wind_degree'], k['temp_c']]])
            hours_array_heat.extend([[k['time'], k['windchill_c'], k['heatindex_c'], k['feelslike_c']]])
    return hours_array_wind, hours_array_heat


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "mohsinbucket1"
    Key = "Toprocess/"

    weather_data = []
    weather_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            weather_data.append(jsonObject)
            weather_keys.append(file_key)

    arr_wind = []
    arr_heat = []
    for dic_weather in weather_data:
        hours_arr_wind, hours_arr_heat = get_data_array(dic_weather)
        arr_wind.extend(hours_arr_wind[0:-1])
        arr_heat.extend(hours_arr_heat[0:-1])

    wind_df = pd.DataFrame.from_dict(arr_wind)
    # wind_df = wind_df.drop_duplicates(subset=['time'])
    wind_key = "transformedv2/wind_data/wind_transformed_" + str(datetime.now()) + ".csv"
    wind_buffer = StringIO()
    wind_df.to_csv(wind_buffer, index=False)
    wind_content = wind_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=wind_key, Body=wind_content)

    heat_df = pd.DataFrame.from_dict(arr_heat)
    # heat_df = heat_df.drop_duplicates(subset=['time'])
    heat_key = "transformedv2/heat_data/heat_transformed_" + str(datetime.now()) + ".csv"
    heat_buffer = StringIO()
    heat_df.to_csv(heat_buffer, index=False)
    heat_content = heat_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=heat_key, Body=heat_content)

    # copy processed file and then delet eit
    s3_resource = boto3.resource('s3')
    for key1 in weather_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key1
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'processed/' + key1.split("/")[-1])
        s3_resource.Object(Bucket, key1.delete())
