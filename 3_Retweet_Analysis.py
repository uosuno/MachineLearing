# Databricks notebook source
# 1. Data is loaded from S3 bucket into a dataframe tweets

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

#Install packages for wordcloud and beautiful soup for web scraping
!pip install nltk
!pip install wordcloud
!pip install beautifulsoup4

# COMMAND ----------

import pyspark.sql.functions as F

# Importing neccessary packages for wordcloud

import nltk
import numpy as np


# COMMAND ----------

# cache the dataframe for faster iteration
tweets_clean.cache() 

# run the count action to materialize the cache
tweets_clean.count()

# COMMAND ----------

display(tweets_clean)

# COMMAND ----------

# MAGIC %sql create database climateDB
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %sql use climateDB

# COMMAND ----------

# Make a table tblClimate, which will be used for any SQL manipulations, and also analyzed in Tableau

tweets_clean.write.mode("overwrite").saveAsTable("tblClimate")


# COMMAND ----------


climate_retweeters = np.array([])
climate_retweeters = (tweets_clean.select('retweeted_person').
      rdd.map(lambda x : x[0]).collect())


print(climate_retweeters) 
  



# COMMAND ----------

from nltk.corpus import stopwords
nltk.download('stopwords')
stop_words = set(stopwords.words('english')) 

retweeters_without_sw = [word for word in climate_retweeters if not word in stop_words and not word == ""]
  
print(retweeters_without_sw)

# COMMAND ----------

from matplotlib.pyplot import figure
from wordcloud import WordCloud

import matplotlib.pyplot as plt
#matplotlib inline
figure(figsize=(12, 12), dpi=80)

# retweeters_without_sw is the list of retweeted persons without stop words, and without null strings

xx = ' '.join(map(str, retweeters_without_sw))
xx = "'" + xx + "'" 

# print(xx)     list of retweeters

wordcloud = WordCloud(width=1280, height=853, margin=0, background_color='#ff1c2a').generate(xx)

plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.margins(x=0, y=0)
plt.show()

plt.savefig("retweeters.jpg")


# COMMAND ----------



# COMMAND ----------

# This data is now cleaned: stop words removed. Put this retweeted persons data in to a dataframe, in the form to be used by external visualizer

# Create a schema for the dataframe
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

retweet_schema = StructType([
    StructField('retweeted_person', StringType(), True)
])

# Convert list to RDD
rdd = sc.parallelize(retweeters_without_sw)
rdd = rdd.map(lambda x:[x]) # transform the rdd

# Create data frame
tblRetweetedPersons = spark.createDataFrame(rdd,retweet_schema)


print(tblRetweetedPersons.schema)
tblRetweetedPersons.show()  




# COMMAND ----------

# MAGIC %sql
# MAGIC use climateDB

# COMMAND ----------

# Save retweeted_persons data frame as databricks table
tblRetweetedPersons.write.mode("overwrite").saveAsTable("tblRetweetedPersons")

# COMMAND ----------

# MAGIC %sql use climatedb

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/climatedb.db/tblmostretweeted',recurse=True)



# COMMAND ----------

# MAGIC %sql create table tblMostRetweeted select retweeted_person, count(retweeted_person) as most_retweeted from tblRetweetedPersons
# MAGIC group by retweeted_person
# MAGIC order by count(retweeted_person) desc
# MAGIC limit 50

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblMostretweeted
