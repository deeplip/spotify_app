# Databricks notebook source
# MAGIC %run ../spotify_app/spotify_utils

# COMMAND ----------

import spotify_modules

dbutils.widgets.text("playlist_id", "","")
playlist_id = dbutils.widgets.get("playlist_id")
# playlist_id = '37i9dQZEVXbMDoHDwVN2tF'

# From child notebook 'spotify_utils'
credentials= Credentials().credentials
playlist_obj = spotify_modules.Playlist(credentials, playlist_id)

playlist_name = playlist_obj.playlist_name
path = Routing('bronze', playlist_name).path

# COMMAND ----------

bronze_df = spark.read.format('delta').load(path)

silver_df = bronze_df.drop('codestring', 'rhythm_version', 'synchstring', 'synch_version', 'analysis_channels', 
               'echoprint_version', 'sample_md5', 'rhythmstring', 'window_seconds',
               'code_version', 'analysis_sample_rate', 'type', 'echoprintstring', 'offset_seconds', 'id')

path = Routing('silver', playlist_name).path
silver_df.write.mode('overwrite').format('delta').save(path)
