# Databricks notebook source
# 1. Data is loaded from S3 bucket into a dataframe tweets

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName('Climate Tweets Sentiment Analysis') \
        .getOrCreate()
print('Session created')

sc = spark.sparkContext

# COMMAND ----------

# MAGIC %fs ls /mnt/climate/
# MAGIC   

# COMMAND ----------

climate_parquet_in = "/mnt/climate/climateDataStream"
tweets = (spark.read.format("parquet")
          .option('header','true')
          .option('delimiter','\t')
          .option("compression", "snappy")
          .parquet(climate_parquet_in)
          )

# COMMAND ----------

# cache the dataframe for faster iteration
tweets.cache() 

# run the count action to materialize the cache
tweets.count()

# COMMAND ----------

display(tweets)

# COMMAND ----------

# 2. Text Cleaning Preprocessing
# pyspark.sql.functions.regexp_replace is used to process the text

# Remove URLs such as http://
# Remove special characters
# Substituting multiple spaces with single space
# Lowercase all text
# Trim the leading/trailing whitespaces

# COMMAND ----------



# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType 

# COMMAND ----------


tweets_clean = tweets.withColumn('retweeted_person', (F.when(  F.instr('tweet','@')==0, F.lit('')).otherwise( ( F.split( ( F.split(F.column('tweet'),'@').getItem(1)), ' ') ).getItem(0)      ).alias('retweeted_person') ) )\
                    .withColumn('retweeted_person', F.regexp_replace(F.lower('retweeted_person'), r":", "") )\
                    .withColumn('tweet_parse', F.regexp_replace('tweet', r"RT @", "").alias('tweet_parse') )\
                    .withColumn('tweet_parse', F.regexp_replace('tweet_parse', r"http\S+", "")) \
                    .withColumn('tweet_parse', F.regexp_replace('tweet_parse', r"[^a-zA-z0-9@]", " ")) \
                    .withColumn('tweet_parse', F.regexp_replace('tweet_parse', r"\s+", " ")) \
                    .withColumn('tweet_parse', F.lower('tweet_parse')) \
                    .withColumn('tweet_parse', F.trim('tweet_parse'))\
                    .withColumn('created_at', F.regexp_replace('created_at', r"\+0000", " ")) \
                    .withColumn('day_of_week', F.substring('created_at',0,3).alias('day_of_week'))\
                    .withColumn('month_name', F.substring('created_at',5,3).alias('month_name'))\
                    .withColumn('month', F.from_unixtime(F.unix_timestamp(F.column('month_name'),'MMM'),'MM').alias('month'))\
                    .withColumn('day', F.substring('created_at',9,2).alias('day'))\
                    .withColumn('hour', F.substring('created_at',12,2).alias('hour'))\
                    .withColumn('minute', F.substring('created_at',15,2).alias('minute'))\
                    .withColumn('second', F.substring('created_at',18,2).alias('second'))\
			        .withColumn('year', F.substring('created_at',23,4).alias('year'))\
                    .withColumn('tweet_date_string', F.concat('year', F.lit('-'), 'month', F.lit('-'), 'day' ).alias('tweet_date_string')) \
                    .withColumn('tweet_time_string', F.concat('hour', F.lit(':'), 'minute', F.lit(':'), 'second' ).alias('tweet_time_string'))\
                    .withColumn('tweet_date', F.from_unixtime(F.unix_timestamp(F.concat('tweet_date_string', F.lit(' '), 'tweet_time_string')), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()).alias('tweet_date') )   
                                

                  
display(tweets_clean)

# COMMAND ----------

tweets_clean.printSchema()

# COMMAND ----------

# Save data cleaned so far on S3
# Put consolidated climate_data back as Paquet, back on the S3 bucket
climate_clean_parquet_out = "/mnt/climate/climateCleanDataStream"

(tweets_clean.write                        
  .option("compression", "snappy")
  .mode("overwrite")                   
  .parquet(climate_clean_parquet_out)            
)

# COMMAND ----------

climate_parquet_in = "/mnt/climate/climateCleanDataStream"
tweets_clean = (spark.read.format("parquet")
          .option('header','true')
          .option('delimiter','\t')
          .option("compression", "snappy")
          .parquet(climate_parquet_in)
          )

# COMMAND ----------

# MAGIC %sql create database climateDB
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %sql use climateDB

# COMMAND ----------

tweets_clean.write.mode("overwrite").saveAsTable("tblClimate")


# COMMAND ----------

# MAGIC %sql
# MAGIC use climateDB

# COMMAND ----------

# MAGIC %sql select count(*) from tblclimate where tweet is  null

# COMMAND ----------

# MAGIC %sql select count(*) from tblclimate where tweet is not null

# COMMAND ----------

# There were 465 tweets with no words after cleaning. Remove all records will null tweets prior to Tokenization

# COMMAND ----------

# MAGIC %sql delete from tblclimate where tweet is null

# COMMAND ----------

climateAll = spark.read.format("delta").load('/user/hive/warehouse/climatedb.db/tblclimate')
climateAll.show()

# COMMAND ----------

# Save retweeted_persons data frame as databricks table
# Save data cleaned with Nulls removed back on S3
# Put consolidated climate_data back as Paquet, back on the S3 bucket
climate_clean_parquet_out = "/mnt/climate/climateCleanDataStream"

(climateAll.write                        
  .option("compression", "snappy")
  .mode("overwrite")                   
  .parquet(climate_clean_parquet_out)            
)
