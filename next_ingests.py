# Databricks notebook source
import spotify_modules
from spotipy.oauth2 import SpotifyClientCredentials

client_id = dbutils.secrets.get(scope = 'SpotifyClientID', key = 'SpotifyClientID')
key = dbutils.secrets.get(scope = 'SpotifySecretKey', key = 'SpotifySecretKey')
credentials = SpotifyClientCredentials(client_id=client_id, client_secret=key)

# dbutils.widgets.text("playlist", "","")
# playlist = dbutils.widgets.get("playlist")

playlist = '37i9dQZF1DXcBWIGoYBM5M' # PARAM
path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_name}"

def get_todays_playlist():
    playlist_df = spotify_modules.Playlist(credentials, playlist).get_df()
    return spark.createDataFrame(playlist_df)

def get_existing_playlist(path):
    container_name = 'bronze'
    file_name = 'top_world'
    storage_name = 'spotify0storage'
    return spark.read.format('delta').load(path)

todays_playlist = get_todays_playlist()
existing_playlist = get_existing_playlist(path)

existing_tracks = existing_playlist.select('track_id')
todays_tracks = todays_playlist.select('track_id')
new_tracks = existing_tracks.subtract(todays_tracks) # For check when playlist change!
new_tracks_exists = new_tracks.count() > 0

# COMMAND ----------

if new_tracks_exists:
    new_tracks_list = new_tracks.select('track_id').rdd.flatMap(lambda x: x).collect()
#     new_tracks_list = ['3uUuGVFu1V7jTQL60S1r8z', '2pIUpMhHL6L9Z5lnKxJJr9', '0T5iIrXA4p5GsubkhuBIKV']
    playlist_from_new_tracks = todays_playlist.where(F.col('track_id').isin(new_tracks_list))
    audio_data_from_new_tracks = spotify_modules.Playlist(credentials, playlist).get_audio_data(playlist_from_new_tracks.select('*').toPandas())
    playlist_from_new_tracks.write.mode('append').format('delta').save(path) #When it differs, try to append to delta.
    playlist_from_new_tracks.display()
