# Databricks notebook source
# MAGIC %run ../spotify_app/spotify_utils

# COMMAND ----------

#Read
dbutils.widgets.text("playlist_id", "","")
playlist_id = dbutils.widgets.get("playlist_id")
table_name = 'Top50-Global//fact_data'
path = Routing('gold', table_name).path
df = spark.read.format('delta').load(path)

# COMMAND ----------

#Flat artist_id array
from pyspark.sql import functions as F
df = (
    df.withColumn('artist_id', F.array_join(F.col('artist_id'), ','))
)

# COMMAND ----------

#Write
file_name = 'flatFactData'
path = Routing('gold', file_name).path
df.write.mode('overwrite').format('csv').save(path)
