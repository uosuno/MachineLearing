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

import pyspark.sql.functions as F

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

# MAGIC %sql drop table tblmedia

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/climatedb.db/tblmedia',recurse=True)

# COMMAND ----------

# MAGIC %sql create table tblMedia select split(hashtags_and_media, ' ') as media_url from tblclimate
# MAGIC where (hashtags_and_media is not null) and instr(hashtags_and_media,'http') >0

# COMMAND ----------

# MAGIC %sql select count(*) from tblMedia

# COMMAND ----------

#Read media table into dataframe
media_tbl = spark.read.format("delta").load('/user/hive/warehouse/climatedb.db/tblmedia')
media_tbl.show()

# COMMAND ----------


media_tags = np.array([])
media_tags = (media_tbl.select('media_url').
      rdd.map(lambda x : x[0]).collect())

media_array = []
for x in media_tags:
    media_array = media_array + x

#hashtag_list = ','.join(map(str, result))   

#print(hashtag_list)
print(media_array)
  

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC use climateDB

# COMMAND ----------

# This data is now cleaned: stop words removed. Put this retweeted persons data in to a dataframe, in the form to be used by external visualizer

# Create a schema for the dataframe
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

media_schema = StructType([
    StructField('media_url', StringType(), True)
])

# Convert list to RDD
rdd = sc.parallelize(media_array)
rdd = rdd.map(lambda x:[x]) # transform the rdd

# Create data frame
tblmedia = spark.createDataFrame(rdd,media_schema)


print(tblmedia.schema)
tblmedia.show()  


# COMMAND ----------

# MAGIC %sql drop table tblmedia

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/climatedb.db/tblmedia',recurse=True)

# COMMAND ----------

# Save media url data frame as databricks table
tblmedia.write.mode("overwrite").saveAsTable("tblmedia")

# COMMAND ----------

# MAGIC %sql delete from tblmedia
# MAGIC where media_url is null or trim(media_url) ="" or instr(media_url,'http') = 0

# COMMAND ----------

# MAGIC %sql update tblmedia
# MAGIC      set media_url = replace(media_url,',','')

# COMMAND ----------

# MAGIC %sql select * from tblmedia

# COMMAND ----------

#Read media table into dataframe
tblmedia = spark.read.format("delta").load('/user/hive/warehouse/climatedb.db/tblmedia')
tblmedia.show()

# COMMAND ----------

tblmedia.write.mode("overwrite").saveAsTable("tblmedia")


# COMMAND ----------

# MAGIC %sql drop table tblmostmedia

# COMMAND ----------

# dbutils.fs.rm('dbfs:/user/hive/warehouse/climatedb.db/tblmostmedia',recurse=True)

# COMMAND ----------

# MAGIC %sql  create table tblMostMedia select count(media_url) as most_media, media_url
# MAGIC from tblmedia
# MAGIC group by media_url
# MAGIC order by count(media_url) desc
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblMostMedia
