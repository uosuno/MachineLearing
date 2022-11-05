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

 # dbutils.fs.rm('dbfs:/user/hive/warehouse/climatedb.db/tblhashtags',recurse=True)

# COMMAND ----------

# MAGIC %sql create table tblHashTags select split(hashtags_and_media, ' ') as hashtags from tblclimate
# MAGIC where (hashtags_and_media is not null) and instr(hashtags_and_media,'#') >0

# COMMAND ----------

# MAGIC %sql select count(*) from tblhashtags

# COMMAND ----------

#Read hashtag table into dataframe
hashtag_tbl = spark.read.format("delta").load('/user/hive/warehouse/climatedb.db/tblhashtags')
hashtag_tbl.show()

# COMMAND ----------


climate_hashtags = np.array([])
climate_hashtags = (hashtag_tbl.select('hashtags').
      rdd.map(lambda x : x[0]).collect())

hashtag_array = []
for x in climate_hashtags:
    hashtag_array = hashtag_array + x

#hashtag_list = ','.join(map(str, result))   

#print(hashtag_list)
print(hashtag_array)
  

# COMMAND ----------



# COMMAND ----------

from matplotlib.pyplot import figure
from wordcloud import WordCloud

import matplotlib.pyplot as plt
#matplotlib inline
figure(figsize=(12, 12), dpi=80)

# hashtag_array is the list of hashtags 

xx = ' '.join(map(str, hashtag_array))
xx = "'" + xx + "'" 

# print(xx)     list of hashtags

wordcloud = WordCloud(width=1280, height=853, margin=0, background_color='#ffff33').generate(xx)

plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.margins(x=0, y=0)
plt.show()

plt.savefig("hashtags.jpg")


# COMMAND ----------

# MAGIC %sql
# MAGIC use climateDB

# COMMAND ----------

# This data is now cleaned: stop words removed. Put this retweeted persons data in to a dataframe, in the form to be used by external visualizer

# Create a schema for the dataframe
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

hashtag_schema = StructType([
    StructField('hashtags', StringType(), True)
])

# Convert list to RDD
rdd = sc.parallelize(hashtag_array)
rdd = rdd.map(lambda x:[x]) # transform the rdd

# Create data frame
tblhashtags = spark.createDataFrame(rdd,hashtag_schema)


print(tblhashtags.schema)
tblhashtags.show()  


# COMMAND ----------

# MAGIC %sql drop table tblhashtags

# COMMAND ----------

# Save retweeted_persons data frame as databricks table. Dropped prior table because it had a different schema. I couldn't overwrite with different schema.
tblhashtags.write.mode("overwrite").saveAsTable("tblHashTags")

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/climatedb.db/tblmosthashtag',recurse=True)

# COMMAND ----------

# MAGIC %sql create table tblMostHashtag select hashtags, count(hashtags) as most_hashtags from tblhashtags
# MAGIC group by hashtags
# MAGIC order by count(hashtags) desc
# MAGIC limit 50

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblMosthashtag
