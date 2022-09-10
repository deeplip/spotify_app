# Databricks notebook source
# MAGIC %run ../modules/spotify_utils

# COMMAND ----------

import pandas as pd

dbutils.widgets.text("playlist_id", "","")
playlist_id = dbutils.widgets.get("playlist_id")
# playlist_id = '37i9dQZF1DX4UtSsGT1Sbe'
credentials= Credentials().credentials
playlist_obj = spotify_modules.Playlist(credentials, playlist_id)

def get_audio_data(credentials, playlist_id, playlist_obj):
    '''
    Returns the Playlist Data Frame.

            Parameters:
                    credentials (Credentials().credentials): Spotify credentials object
                    playlist_id (str) : playlist id from pipeline @variable('playlist_id')
                    playlist_obj (Playlist()) : Spotify playlist object
            Returns:
                    spark_df_audio_data (spark.DataFrame): Tracks playlist spark.DataFrame
    '''
    audio_df = playlist_obj.get_audio_data()
    spark_df_audio_data = spark.createDataFrame(audio_df)
    return spark_df_audio_data

def save_audio_data(spark_df_audio_data, playlist_obj):
    '''
    Save the Playlist Data Frame in Delta Lake spotify0storage/bronze/<playlist_name> in first ingest (no file on storage).

            Parameters:
                    spark_df_audio_data (spark.DataFrame): Tracks playlist spark.DataFrame
                    playlist_obj (Playlist()) : Spotify playlist object

            Returns:
                    spark_df_audio_data (spark.DataFrame): Tracks playlist spark.DataFrame
    '''
    playlist_name = playlist_obj.playlist_name
    path = Routing('bronze', playlist_name).path
    spark_df_audio_data.write.format('delta').save(path)
    spark_df_audio_data.display()
    return spark_df_audio_data

spark_df_audio_data = get_audio_data(credentials, playlist_id, playlist_obj)
save_audio_data(spark_df_audio_data, playlist_obj)
