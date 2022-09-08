# Databricks notebook source
# MAGIC %run ../spotify_app/get_params

# COMMAND ----------

import spotify_modules
import pandas as pd

dbutils.widgets.text("playlist_id", "","")
playlist_id = dbutils.widgets.get("playlist_id")
# playlist_id = get_playlist_id()

# From child notebook 'spotify_utils'
credentials= Credentials().credentials

playlist_obj = spotify_modules.Playlist(credentials, playlist_id)
audio_df = playlist_obj.get_audio_data()
spark_df_audio_data = spark.createDataFrame(audio_df)

# COMMAND ----------

playlist_name = playlist_obj.playlist_name
path = Routing('bronze', playlist_name).path
spark_df_audio_data.write.format('delta').save(path)
