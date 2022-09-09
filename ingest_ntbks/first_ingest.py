# Databricks notebook source
# MAGIC %run ../modules/spotify_utils

# COMMAND ----------

import pandas as pd


dbutils.widgets.text("playlist_id", "","")
playlist_id = dbutils.widgets.get("playlist_id")

# From child notebook 'spotify_utils'
credentials= Credentials().credentials

playlist_obj = spotify_modules.Playlist(credentials, playlist_id)
audio_df = playlist_obj.get_audio_data()
spark_df_audio_data = spark.createDataFrame(audio_df)

# COMMAND ----------

playlist_name = playlist_obj.playlist_name
path = Routing('bronze', playlist_name).path
spark_df_audio_data.write.format('delta').save(path)
