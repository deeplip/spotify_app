# Databricks notebook source
# MAGIC %run ../spotify_app/spotify_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create params

# COMMAND ----------

import spotify_modules

credentials= Credentials().credentials
playlist_name = lambda id: spotify_modules.Playlist(credentials, id).playlist_name

playlists = (
    ("37i9dQZF1DX4UtSsGT1Sbe", f"{playlist_name('37i9dQZF1DX4UtSsGT1Sbe')}"),
    ("37i9dQZEVXbN6itCcaL3Tt", f"{playlist_name('37i9dQZEVXbN6itCcaL3Tt')}"),
    ("37i9dQZEVXbMDoHDwVN2tF", f"{playlist_name('37i9dQZEVXbMDoHDwVN2tF')}"),
    ("37i9dQZF1DXbITWG1ZJKYt", f"{playlist_name('37i9dQZF1DXbITWG1ZJKYt')}")
)

df = spark.createDataFrame(playlists,schema = ['playlist_id', 'playlist_name'])
path = Routing('params','config').path
df.write.mode('overwrite').format('parquet').save(path)
