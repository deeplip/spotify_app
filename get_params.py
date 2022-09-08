# Databricks notebook source
# MAGIC %run ../spotify_app/spotify_utils

# COMMAND ----------

def get_playlist_id():
    path = Routing('params', 'runs').path
    params_df = spark.read.format('json').load(path)
    return params_df.select('playlist_id').rdd.flatMap(lambda x: x).collect()[0]

def get_playlist_name():
    path = Routing('params', 'runs').path
    params_df = spark.read.format('json').load(path)
    return params_df.select('playlist_name').rdd.flatMap(lambda x: x).collect()[0]
