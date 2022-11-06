# Databricks notebook source
# Climate Twitter Chatter Analysis
# Almost 794, 803  collected 
#
#
# Original data collected in the schem detailed below as climateSchema

# COMMAND ----------

# Big Data Capstone Project - Climate Change Data Analysis by Uzo Osunp
# This project will extract twitter feeds at different times of the day over a week in May 2022
# The fields extracted are user details: Id, userName, screen_name, verified, location, followersCount
# Tweet details: favoriteCount, retweetCount, createdAt, HashTagList, MediaUrlList, countryCode

# COMMAND ----------

def mount_s3_bucket(access_key, secret_key, bucket_name, mount_folder):
  ACCESS_KEY_ID = access_key
  SECRET_ACCESS_KEY = secret_key
  ENCODED_SECRET_KEY = SECRET_ACCESS_KEY.replace("/", "%2F")

  print ("Mounting", bucket_name)

  try:
    # Unmount the data in case it was already mounted.
    dbutils.fs.unmount("/mnt/%s" % mount_folder)
    
  except:
    # If it fails to unmount it most likely wasn't mounted in the first place
    print ("Directory not unmounted: ", mount_folder)
    
  finally:
    # Lastly, mount our bucket.
    dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY_ID, ENCODED_SECRET_KEY, bucket_name), "/mnt/%s" % mount_folder)
    #dbutils.fs.mount("s3a://"+ ACCESS_KEY_ID + ":" + ENCODED_SECRET_KEY + "@" + bucket_name, mount_folder)
    print ("The bucket", bucket_name, "was mounted to", mount_folder, "\n")
    

# COMMAND ----------

# Set AWS programmatic access credentials
ACCESS_KEY = "XXXXXXXXX"
SECRET_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXX"

mount_s3_bucket(ACCESS_KEY, SECRET_ACCESS_KEY, 'profbubbles/twitter_climate/','climate')


# COMMAND ----------

display(dbutils.fs.ls("/mnt/climate/"))



# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, FloatType

climateSchema = StructType([
StructField('tweetid',LongType(), True),
StructField('user_name',StringType(), True),
StructField('screen_name',StringType(), True),
StructField('verified',StringType(), True),
StructField('user_location',StringType(), True),
StructField('userId',LongType(), True),
StructField('followers',LongType(), True),
StructField('tweet',StringType(), True),
StructField('favorite_count',IntegerType(), True),
StructField('retweet_count',IntegerType(), True),
StructField('created_at',StringType(), True),
StructField('hashtags_and_media',StringType(), True),
StructField('place_type',StringType(), True),
StructField('country',StringType(), True),
StructField('country_code',StringType(), True),
StructField('region',StringType(), True),
StructField('place',StringType(), True)
])

# COMMAND ----------

climate_data = (spark.read
          .option('header','false')
          .option('delimiter','\t')
          .schema(climateSchema)
          .csv('/mnt/climate/*/*/*/*/*')
          )

# COMMAND ----------

climate_data.count()

# COMMAND ----------

climate_data.show(5)

# COMMAND ----------

# DBTITLE 1,Print Climate Raw Data Schema 
climate_data.printSchema()

# COMMAND ----------

# Saved Raw Data both as CSV and PARQUET
# I will be using the PARQUET files for this project

# COMMAND ----------

# Output path for Original Climate Data saved as a consolidated CSV frame
climate_csv_out = "/mnt/climate/climateDataStreamCSV"

# COMMAND ----------

# Put consolidated climate_data as CSV, back on the S3 bucket

(climate_data.write                       # Our DataFrameWriter
  .option("delimiter", "\t")  
  .option("header", "true")
  .mode("overwrite")               # Replace existing files
  .csv(climate_csv_out)               # Write DataFrame to csv files
)

# COMMAND ----------

# Put consolidated climate_data back as Paquet, back on the S3 bucket
climate_parquet_out = "/mnt/climate/climateDataStream"

(climate_data.write                       # Our DataFrameWriter
  .option("delimiter", "\t")  
  .option("compression", "snappy")
  .mode("overwrite")                  # Replace existing files
  .parquet(climate_parquet_out)            # Write DataFrame to parquet files
)
