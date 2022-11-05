# Databricks notebook source
# 1. Cleaned Data from (Notebook 2) is loaded from S3 bucket into a dataframe tweets_clean

# COMMAND ----------

# MAGIC %fs ls /mnt/climate/
# MAGIC   

# COMMAND ----------

climate_parquet_in = "/mnt/climate/climateCleanDataStream"
tweets_clean = (spark.read.format("parquet")
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

# Install important libraries
!pip install -U textblob


# COMMAND ----------

from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer,HashingTF, IDF,StringIndexer, NGram, ChiSqSelector, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
import numpy as np
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer

# COMMAND ----------

# cache the dataframe for faster iteration
tweets_clean.cache() 

# run the count action to materialize the cache
tweets_clean.count()

# COMMAND ----------

display(tweets_clean)

# COMMAND ----------

tweets_clean = tweets_clean.withColumn("polarity", (F.lit(0).cast('float')).alias('polarity') )  \
                           .withColumn("subjectivity", (F.lit(0).cast('float')).alias('subjectivity') )

# COMMAND ----------

display(tweets_clean)

# COMMAND ----------

tweets_clean.printSchema()

# COMMAND ----------



# COMMAND ----------

# Collecting the tweets and created polarity and subjectivity columns as a list of thruples

tweets  = np.array([])
tweets = (tweets_clean.select('tweet_parse','polarity','subjectivity').
      rdd.map(lambda x : x[0:3]).collect())


print(tweets) 
  


# COMMAND ----------

# polarity between -1 and 1 ------- -1 is negative 0 is neutral 1 is positive
# I am going to create categories for very negative E (-1 >= x < -0.5), negative D (-0.5 >= x < 0), 
# neutral C (x = 0), positive B(0 > x <= 0.6), very positive A ( x > 0.6)
# subjectivity 0 and 1 ----  0 = objective 1 = subjective (0 >= x < 0.4 objective, 0.4 >= x < 0.6 fair, 0.6 >= x <= 1 = subjective)

# COMMAND ----------

# Use textblob to generate sentiment classification
tweets_new = []
for x in tweets:
    (tweet, polarity,subjectivity) = x
    polarity = TextBlob(tweet).polarity
    subjectivity = TextBlob(tweet).subjectivity
    if polarity >= -1 and polarity < -0.5:
        sentiment = 'Very Negative'
    elif polarity >= -0.5 and polarity < 0:
        sentiment = 'Negative'
    elif polarity == 0:
        sentiment = 'Neutral'
    elif polarity > 0 and polarity <= 0.6:
        sentiment = 'Positive'
    else:
        sentiment = 'Very Positive'
        
    if subjectivity >= 0 and subjectivity < 0.4:
        context = 'Objective'
    elif subjectivity >= 0.4 and subjectivity < 0.6:
        context = 'Neutral'
    else:
        context = 'Subjective'
        
    y = (tweet, polarity, subjectivity, sentiment, context)
    tweets_new.append(y)
    

# COMMAND ----------

display(tweets_new)

# COMMAND ----------

# Tweets with assigned textblob determined subjectivity and polarity. These will be put in a dataframe and analyzed to determine sentiment

# Create a schema for the dataframe
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType

tweet_schema = StructType([
    StructField('tweet', StringType(), True),
    StructField('polarity', FloatType(), True),
    StructField('subjectivity', FloatType(), True),
    StructField('sentiment', StringType(), True),
    StructField('context', StringType(), True)
])

# Convert list to RDD
#rdd = sc.parallelize(tweets)
#rdd = rdd.map(lambda x:[x]) # transform the rdd

# Create data frame
tblTweetSentiments = spark.createDataFrame(tweets_new,tweet_schema)


print(tblTweetSentiments.schema)



# COMMAND ----------

# Save Tweets, Polarity sentiment and Subjectivity context in S3 bucket
 
climate_clean_parquet_out = "/mnt/climate/climateTweetSentimentStream"

( tblTweetSentiments.write                        
  .option("compression", "snappy")
  .mode("overwrite")                   
  .parquet(climate_clean_parquet_out)            
)

# COMMAND ----------

# MAGIC %sql create database climateDB
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %sql use climateDB

# COMMAND ----------

# MAGIC %sql drop table tbltweetsentiments

# COMMAND ----------

tblTweetSentiments.write.mode("overwrite").saveAsTable("tblTweetSentiments")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbltweetsentiments

# COMMAND ----------


