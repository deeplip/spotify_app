# Databricks notebook source
# MAGIC %run ../modules/spotify_utils

# COMMAND ----------

# r../modules/spotify_utils |provides ->| Credentials, Routing, get_param_widget
from pyspark.sql import functions as F

def get_todays_playlist(playlist_obj):
    '''
    Returns the today tracks Playlist Data Frame.

            Parameters:
                    playlist_obj (Playlist): Playlist object

            Returns:
                    spark_playlist_df (spark.DataFrame): Todays tracks playlist spark.DataFrame
    '''
    playlist_df = playlist_obj.get_df()
    spark_playlist_df = spark.createDataFrame(playlist_df)
    return spark_playlist_df

def get_existing_playlist(path):
    '''
    Returns the existing tracks Playlist Data Frame from Delta Lake.

            Parameters:
                    path (String): path to Delta Lake 'delta' file

            Returns:
                    spark_existing_playlist_df (spark.Dataframe): existing tracks playlist spark.DataFrame
    '''
    container_name = 'bronze'
    storage_name = 'spotify0storage'
    spark_existing_playlist_df = spark.read.format('delta').load(path)
    return spark_existing_playlist_df

credentials= Credentials().credentials
# playlist_id = get_param_widget('playlist_id') # Processing playlist's id obtained from pipeline variables
playlist_id = '37i9dQZEVXbMDoHDwVN2tF'
todays_playlist_obj = spotify_modules.Playlist(credentials, playlist_id)
todays_playlist_df = get_todays_playlist(todays_playlist_obj)

path = Routing('bronze', todays_playlist_obj.playlist_name).path
existing_playlist_df = get_existing_playlist(path)

def get_new_tracks(existing_playlist_df, todays_playlist_df):
    '''
    Returns a new tracks spark Data Frame.

            Parameters:
                    existing_playlist_df (spark.DataFrame): Existing tracks playlist spark.Dataframe
                    todays_playlist_df   (spark.DataFrame): Todays tracks playlist spark.Dataframe
                    
            Returns:
                    new_tracks_df (spark.DataFrame): Only newly appeared tracks playlist ids spark.DataFrame
    '''
    existing_tracks = existing_playlist_df.select('track_id')
    todays_tracks = todays_playlist_df.select('track_id')
    new_tracks_df = todays_tracks.subtract(existing_tracks)
    return new_tracks_df

new_tracks = get_new_tracks(existing_playlist_df, todays_playlist_df)

# COMMAND ----------

def concat_track_artist(df):
    '''
    Returns spark Data Frame new column artist_track_concat (concatenated str of artist col and track col). It is used for double check of duplicated tracks.

            Parameters:
                    df (spark.DataFrame): spark.Dataframe with str cols track and artist
                    
            Returns:
                    new_tracks_df (spark.DataFrame): spark.DataFrame with concatenated str of artist col and track col
    '''
    return (df
         .withColumn('artist_track_concat', F.col('artist_name')
                     .getItem(0))
         .withColumn('artist_track_concat', F.concat_ws(' ', F.col('artist_track_concat'), F.col('track_name')))
    )
    
def append_new_tracks(new_tracks, todays_playlist_df, existing_playlist_df):
    '''
    Returns spark Data Frame only for new tracks, also appends it to Delta Lake.

            Parameters:
                    new_tracks           (spark.DataFrame): spark.Dataframe of new tracks (ids only)
                    existing_playlist_df (spark.DataFrame): Existing tracks playlist spark.Dataframe
                    todays_playlist_df   (spark.DataFrame): Todays tracks playlist spark.Dataframe
                    
            Returns:
                    playlist_from_new_tracks (spark.DataFrame): spark.DataFrame of only newly appeared tracks playlist spark.DataFrame with all features.
    '''
    new_tracks_exists = new_tracks.count() > 0
    if new_tracks_exists:
        new_tracks_list = new_tracks.select('track_id').rdd.flatMap(lambda x: x).collect()
        playlist_from_new_tracks = todays_playlist_df.where(F.col('track_id').isin(new_tracks_list))
        playlist_from_new_tracks = concat_track_artist(playlist_from_new_tracks)
        existing_artist_track_concat_ls = concat_track_artist(existing_playlist_df).select('artist_track_concat').rdd.flatMap(lambda x: x).collect()
        playlist_from_new_tracks = playlist_from_new_tracks.where(~F.col('artist_track_concat').isin(existing_artist_track_concat_ls)).drop('artist_track_concat')
        audio_data_from_new_tracks = spotify_modules.Playlist(credentials, playlist_id).get_audio_data(playlist_from_new_tracks.select('*').toPandas())
        audio_data_from_new_tracks.write.mode('append').format('delta').save(path) #SAVE
        audio_data_from_new_tracks.display() #DISPLAY
        return audio_data_from_new_tracks
        
playlist_from_new_tracks = append_new_tracks(new_tracks, todays_playlist_df, existing_playlist_df)
