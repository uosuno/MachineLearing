# Databricks notebook source
# 1. Cleaned Data from (Notebook 2) is loaded from S3 bucket into a dataframe tweets_clean

# COMMAND ----------

# MAGIC %fs ls /mnt/climate/
# MAGIC   

# COMMAND ----------

climate_parquet_in = "/mnt/climate/climateTweetSentimentStream"
tweets_sentiments = (spark.read.format("parquet")
          .option('header','true')
          .option('delimiter','\t')
          .option("compression", "snappy")
          .parquet(climate_parquet_in)
          )

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName('Climate Tweets Sentiment Analysis') \
        .getOrCreate()
print('Session created')

sc = spark.sparkContext

# COMMAND ----------

display(tweets_sentiments)

# COMMAND ----------

from pyspark.ml.feature import NGram, VectorAssembler, StopWordsRemover, HashingTF, IDF, Tokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer

# Use 90% cases for training, 10% cases for testing
train, test = tweets_sentiments.randomSplit([0.7, 0.3], seed=20221120)

# Create transformers for the ML pipeline
tokenizer = Tokenizer(inputCol="tweet", outputCol="tokens")
stopword_remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
cv = CountVectorizer(vocabSize=2**16, inputCol="filtered", outputCol='cv')
idf = IDF(inputCol='cv', outputCol="1gram_idf", minDocFreq=5) #minDocFreq: remove sparse terms
assembler = VectorAssembler(inputCols=["1gram_idf"], outputCol="features")
label_encoder= StringIndexer(inputCol = "sentiment", outputCol = "label")
lr = LogisticRegression(maxIter=100)
pipeline = Pipeline(stages=[tokenizer, stopword_remover, cv, idf, assembler, label_encoder, lr])

pipeline_model = pipeline.fit(train)
predictions = pipeline_model.transform(test)

evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label')
accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(test.count())
roc_auc = evaluator.evaluate(predictions)

print("Accuracy Score: {0:.4f}".format(accuracy))
print("ROC-AUC: {0:.4f}".format(roc_auc))

# COMMAND ----------



# COMMAND ----------

display(predictions)

# COMMAND ----------

# MAGIC %sql create database climateDB
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %sql use climateDB

# COMMAND ----------

predictions.write.mode("overwrite").saveAsTable("tblSentimentPredictions")

# COMMAND ----------

# Save Predictions back in S3 bucket
 
climate_clean_parquet_out = "/mnt/climate/climateTweetPredictions"

( predictions.write                        
  .option("compression", "snappy")
  .mode("overwrite")                   
  .parquet(climate_clean_parquet_out)            
)
