# Databricks notebook source
# MAGIC %run ../spotify_app/spotify_utils

# COMMAND ----------

def pipeline_run(playlist_name):
    from pyspark.sql import functions as F
    #Get df
    path_to_load = Routing('params','config').path
    df = spark.read.format('parquet').load(path_to_load)

    params = df.filter(F.col('playlist_name') == playlist_name)
    if params.count() == 1:
        path_to_save = Routing('params','runs').path
        params.write.mode('overwrite').format('json').save(path_to_save)
        return True
    else: raise Exception("Wrong playlist name.")
