from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import Tokenizer,CountVectorizer,HashingTF,IDF,StringIndexer,IndexToString
from pyspark.ml.classification import NaiveBayesModel
from pyspark.streaming import StreamingContext
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType

path = 'hot_files'
sc =SparkContext()
sc.setLogLevel("ERROR")
spark=SparkSession(sc)

csv_sdf=spark.readStream.schema("Categories STRING, Heading STRING").csv(path)
pipeline_model=PipelineModel.load("pipeline")
nb_model=NaiveBayesModel.load("TFIDF_NB_model")
processed_data=pipeline_model.transform(csv_sdf)
prediction=nb_model.transform(processed_data)

inverter =IndexToString(inputCol="prediction", outputCol="prediction_inverted", labels=pipeline_model.stages[-1].labels)

inverted_prediction=inverter.transform(prediction)
result=inverted_prediction.select(["Heading","Categories","probability","prediction","prediction_inverted"])

# query=result.writeStream.format("console").start()
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.toPandas().to_csv("streaming_result.csv",mode="a")
    pass
query=result.writeStream.foreachBatch(foreach_batch_function).start()
query.awaitTermination()