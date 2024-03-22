#import happybase
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
        "Spark_ML_train_model").getOrCreate()  # ("local[*]", "FirstDemo")
    spark.sparkContext.setLogLevel("ERROR")
    query="Select * from windpredictionproject_jan23.weather_Api_data"
    new_df = spark.sql(query)
    model_loaded = RandomForestRegressionModel().load("ml_model")
    testData=new_df.rdd.map(lambda r:[0,Vectors.dense(r[2:6])]).toDF(['label', 'features'])
    pred=model_loaded.transform(testData)
    pred.show()

        #df_forhbase=spark.createDataFrame([[date_time, pred_power]], ["datetime", "active_power"])
        #df_forhbase.show()






#connection = happybase.Connection('hostname')
#table = connection.table('ml_windpowerpred')

#table.put(b'row-key', {b'family:datetime': b'datetime',
    #                   b'family:power': b'power'})

#row = table.row(b'row-key')
#print(row[b'family:qual1'])  # prints 'value1'

#for key, data in table.rows([b'row-key-1', b'row-key-2']):
#    print(key, data)  # prints row key and data for each row

#for key, data in table.scan(row_prefix=b'row'):
 #   print(key, data)  # prints 'value1' and 'value2'

#row = table.delete(b'row-key')