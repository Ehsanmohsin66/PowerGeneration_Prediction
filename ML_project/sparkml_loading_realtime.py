#import happybase
from pyspark.sql import SparkSession
import logging
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.regression import RandomForestRegressionModel,RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
import json
from pyspark.sql.functions import date_format, to_json, struct, from_json, array
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StructType, StructField, StringType
import pprint



def transData(data):
    return data.rdd.map(lambda r: [r[4], Vectors.dense(r[0:4])]). \
        toDF(['label', 'features'])

def processRow(rowdf):
    for df in rowdf:
        print(df)
    #for df in rowdf:
    #   testdata=df.rdd.map(lambda r: [0, Vectors.dense(r[2:6])]).toDF(['label', 'features'])
        #pred = model_loaded.transform(testdata)
    #write.mode("append").format("csv").path("testdata_group1.csv")
class VarcharType:
    pass


if __name__ == '__main__':
    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)
    spark = SparkSession.builder.appName(
        "Spark_ML_train_model").getOrCreate()  # ("local[*]", "FirstDemo")
    spark.sparkContext.setLogLevel("ERROR")

    TOPIC = "windpowerproject"

    model_loaded = RandomForestRegressionModel().load("ml_model")
    #for testing local vs non local
    BOOSTRAP_SERVER = "ip-172-31-13-101.eu-west-2.compute.internal:9092"
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOSTRAP_SERVER)\
        .option("subscribe", TOPIC).option("startingOffsets", "latest").load()


    df = df.selectExpr("CAST(value AS STRING)")


    schema = StructType([
        StructField("country", StringType(), True),
        StructField("name", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("wa_c", DoubleType(), True),
        StructField("tempt_c", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True)
    ])

    model_loaded = RandomForestRegressionModel().load("ml_model")

    df = df.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

    df_new = df.withColumn("datetime", date_format(col("datetime"), "D")).withColumn("datetime", col("datetime").cast(IntegerType()))

    #quer2 = df_new.writeStream.foreach(processRow).start()

    query = df_new.writeStream.outputMode("append").format("csv").option("path", "UKJan/Group1/Weather_Api_data")\
        .trigger(processingTime='60 seconds').option("checkpointLocation","UKJan/Group1/chkpoint").start()

    query.awaitTermination()





