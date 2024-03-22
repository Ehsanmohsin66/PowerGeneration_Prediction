#import happybase
from os import wait
from time import sleep

from pyspark.sql import SparkSession
import logging
from pyspark.ml.regression import RandomForestRegressionModel,RandomForestRegressor
from pyspark.ml.linalg import Vectors
def transData(data):
    return data.rdd.map(lambda r: [r[4], Vectors.dense(r[0:4])]). \
        toDF(['label', 'features'])


class VarcharType:
    pass


if __name__ == '__main__':
    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)
    spark = SparkSession.builder.appName(
        "Spark_transform").getOrCreate()  # ("local[*]", "FirstDemo")
    spark.sparkContext.setLogLevel("ERROR")

    while True:
        query='SELECT to_date(vw.datetime) as date1,\
        vw.country,vw.region,\
        vw.wind_speed,pd.active_power \
        from windpredictionproject_jan23.weather_api_data_v2 as vw \
        JOIN windpredictionproject_jan23.pred_data as pd ON pd.datetime=vw.datetime \
        where to_date(vw.datetime) >= to_date(current_timestamp()) \
        Order by date1 '
        new_df = spark.sql(query)
        new_df.write.mode("Overwrite").csv("UKJan/Group1/analyzed_data")
        sleep(60)


