from pandas.core.indexes.base import Index
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import Tokenizer,CountVectorizer,HashingTF,IDF,StringIndexer,IndexToString
from pyspark.ml.classification import NaiveBayesModel
from kafka import KafkaConsumer
from kafka import KafkaProducer

kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')

sc =SparkContext()
spark=SparkSession(sc)
sc.setLogLevel("ERROR")

df=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "news-raw").load()


df=df.selectExpr("CAST(key AS STRING) as Categories", "CAST(value AS STRING) as Heading")
pipeline_model=PipelineModel.load("pipeline")
nb_model=NaiveBayesModel.load("TFIDF_NB_model")
processed_data=pipeline_model.transform(df)
prediction=nb_model.transform(processed_data)

inverter =IndexToString(inputCol="prediction", outputCol="prediction_inverted", labels=pipeline_model.stages[-1].labels)

inverted_prediction=inverter.transform(prediction)
result=inverted_prediction.select(["Heading","Categories","probability","prediction","prediction_inverted"])

def foreach_batch_function(df, epoch_id):
    for idx,row in df.toPandas().iterrows():
        kafka_producer.send(f'news-{row.prediction_inverted}',value=bytes(row.Heading,encoding="UTF-8"))
    df.toPandas().to_csv("streaming_result.csv",mode="a",index=False)

query=result.writeStream.foreachBatch(foreach_batch_function).start()
query.awaitTermination()
