# Databricks notebook source
# Databricks notebook source
import spotify_modules
import pandas as pd

dbutils.widgets.text("playlist", "","")
playlist = dbutils.widgets.get("playlist")

# playlist = '37i9dQZF1DXcBWIGoYBM5M' # PARAM

playlist_df = spotify_modules.Playlist(playlist).get_df()
audio_df = get_audio_data(playlist_df)
spark_df_audio_data = spark.createDataFrame(audio_df)

# COMMAND ----------

container_name = 'bronze'
file_name = 'top_world'
storage_name = 'spotify0storage'
path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_name}"

spark_conf_set(storage_name)
spark_df_audio_data.write.format('delta').save(path)
