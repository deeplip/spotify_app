# Databricks notebook source
from spotipy.oauth2 import SpotifyClientCredentials

client_id = dbutils.secrets.get(scope = 'SpotifyClientID', key = 'SpotifyClientID')
key = dbutils.secrets.get(scope = 'SpotifySecretKey', key = 'SpotifySecretKey')
credentials = SpotifyClientCredentials(client_id=client_id, client_secret=key)

import spotify_modules
import pandas as pd

# dbutils.widgets.text("playlist", "","")
# playlist = dbutils.widgets.get("playlist")

playlist = '37i9dQZF1DXcBWIGoYBM5M' # PARAM

playlist_obj = spotify_modules.Playlist(credentials, playlist)
audio_df = playlist_obj.get_audio_data()
print(audio_df.info())
spark_df_audio_data = spark.createDataFrame(audio_df)

# COMMAND ----------

playlist_obj.get_df()

# COMMAND ----------

container_name = 'bronze'
file_name = 'top_world'
storage_name = 'spotify0storage'
path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_name}"

spark_conf_set(storage_name)
spark_df_audio_data.write.format('delta').save(path)

# COMMAND ----------



# COMMAND ----------


