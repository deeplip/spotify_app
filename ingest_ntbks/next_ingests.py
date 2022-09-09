# Databricks notebook source
# MAGIC %run ../modules/spotify_utils

# COMMAND ----------

# r../modules/spotify_utils |provides ->| Credentials, Routing, get_param_widget


playlist_id = get_param_widget('playlist_id')
credentials= Credentials().credentials
todays_playlist_obj = spotify_modules.Playlist(credentials, playlist_id)
playlist_name = todays_playlist_obj.playlist_name
path = Routing('bronze', playlist_name).path

def get_todays_playlist(playlist_obj):
    playlist_df = playlist_obj.get_df()
    return spark.createDataFrame(playlist_df)

def get_existing_playlist(path):
    container_name = 'bronze'
    file_name = playlist_name
    storage_name = 'spotify0storage'
    return spark.read.format('delta').load(path)

todays_playlist = get_todays_playlist(todays_playlist_obj)
existing_playlist = get_existing_playlist(path)

existing_tracks = existing_playlist.select('track_id')
todays_tracks = todays_playlist.select('track_id')
new_tracks = todays_tracks.subtract(existing_tracks) # For check when playlist change!
new_tracks_exists = new_tracks.count() > 0

# COMMAND ----------

def concat_track_artist(df):
    return (df
         .withColumn('artist_track_concat', F.col('artist_name')
                     .getItem(0))
         .withColumn('artist_track_concat', F.concat_ws(' ', F.col('artist_track_concat'), F.col('track_name')))
    )
    
if new_tracks_exists:
    from pyspark.sql import functions as F
    new_tracks_list = new_tracks.select('track_id').rdd.flatMap(lambda x: x).collect()
    playlist_from_new_tracks = todays_playlist.where(F.col('track_id').isin(new_tracks_list))
    playlist_from_new_tracks = concat_track_artist(playlist_from_new_tracks)
    existing_artist_track_concat_ls = concat_track_artist(existing_playlist).select('artist_track_concat').rdd.flatMap(lambda x: x).collect()
    playlist_from_new_tracks = playlist_from_new_tracks.where(~F.col('artist_track_concat').isin(existing_artist_track_concat_ls)).drop('artist_track_concat')
    audio_data_from_new_tracks = spotify_modules.Playlist(credentials, playlist_id).get_audio_data(playlist_from_new_tracks.select('*').toPandas())
    playlist_from_new_tracks.write.mode('append').format('delta').save(path) #When it differs, try to append to delta.
    playlist_from_new_tracks.display()

