from pyspark.ml.feature import IndexToString
from pyspark.sql import SparkSession
import logging
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.regression import RandomForestRegressionModel,RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

def transData(data):
    return data.rdd.map(lambda r: [r[4], Vectors.dense(r[0:4])]).toDF(['label', 'features'])

if __name__ == '__main__':
    s_logger = logging.getLogger('py4j.java_gateway')
    s_logger.setLevel(logging.ERROR)
    spark = SparkSession.builder.appName(
        "Spark_ML_train_model").enableHiveSupport().getOrCreate()  # ("local[*]", "FirstDemo")
    spark.sparkContext.setLogLevel("ERROR")

    tables = spark.sql("show tables").show()
    data1=spark.sql("Select  date_time as feature1,ot_avg as feature2,wa_c_avg as feature3,\
    wind_speed as feature4,\
    case when p_avg < 0 then 0 else p_avg end as label1,q_avg as label2 \
    from windpredictionproject_jan23.fact_truth_table where p_avg is NOT NULL")
    data_1=data1.withColumn("feature1", date_format(col("feature1"), "D"))\
        .withColumn("feature1",col("feature1").cast(IntegerType()))
    data_1.printSchema()
    data2 = transData(data_1)
    (trainingData, testData) = data2.randomSplit([0.8, 0.2])
    print("Number of rows in training data are: " + str(trainingData.count()))
    print("Number of rows in test data are: " + str(testData.count()))
    # create the trainer and set its parameters


    rf=RandomForestRegressor().setLabelCol("label").setFeaturesCol("features")
    model = rf.fit(trainingData)
    model.save("ml_model")


    #model_loaded=RandomForestRegressor().load("Wind_Project\\ml_model")
    #pred=model_loaded.transform(testData)
    #pred.select("prediction", "label", "features").show(5)
    #mlp = MultilayerPerceptronClassifier(labelCol="label",\
    #                                    featuresCol="features",\
    #                                     maxIter=25,layers=[4, 6, 2], seed=123)

    # Convert indexed labels back to original labels.
    #labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",labels=label.labels)
    # Chain indexers and forest in a Pipeline


    #pipeline = Pipeline(stages=[labelIndexer, featureIndexer, FNN, labelConverter])
    # train the model
    # Train model.  This also runs the indexers.
    #model = mlp.fit(trainingData)
    #model.save("C:\\Users\\ehsan\\OneDrive\\Desktop\\Wind_Project\\ml_model")



