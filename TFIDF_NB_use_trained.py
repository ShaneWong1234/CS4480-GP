from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer,CountVectorizer,HashingTF,IDF,StringIndexer,IndexToString
from pyspark.ml.classification import NaiveBayesModel

sc =SparkContext()
spark=SparkSession(sc)
# path = 'hdfs://10.1.0.4:9000/Combined_Dataset.csv'
path = 'Combined_Dataset.csv'
data = spark.read.csv(path, header=True,inferSchema=True,mode="DROPMALFORMED")
news_heading = data.select(["Heading","Categories"]).filter(F.col("Heading").isNotNull()).filter(F.col("Categories").isNotNull()).filter(F.col("Categories").isin(['business', 'lifestyle', 'science', 'sports', 'technology', 'wealth']))

tokenizer=Tokenizer(inputCol="Heading", outputCol="Heading_token")
hashing_TF=HashingTF(inputCol="Heading_token", outputCol="Heading_tf")
idf=IDF(inputCol="Heading_tf", outputCol="Heading_tfidf")
str_idxer = StringIndexer(inputCol = "Categories", outputCol = "Categories_idx")
pipeline = Pipeline(stages=[tokenizer, hashing_TF,idf,str_idxer])
pipeline_model = pipeline.fit(news_heading)
dataset = pipeline_model.transform(news_heading)
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 42)



nb_model=NaiveBayesModel.NaiveBayesModel.load("TFIDF_NB_model")
prediction=nb_model.transform(testData)

inverter =IndexToString(inputCol="prediction", outputCol="prediction_inverted", labels=pipeline_model.stages[-1].labels)

inverted_prediction=inverter.transform(prediction)
result=inverted_prediction.select(["Heading","Categories","probability","prediction","prediction_inverted"])
# result.toPandas().to_csv('hdfs://10.1.0.4:9000/result.csv',index=False)
result.toPandas().to_csv('trained_model_result.csv',index=False)