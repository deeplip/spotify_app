# Databricks notebook source
# MAGIC %run ../modules/spotify_utils

# COMMAND ----------

dbutils.widgets.text("playlist_id", "","")
playlist_id = dbutils.widgets.get("playlist_id")
playlist_id = '37i9dQZF1DX4UtSsGT1Sbe'
credentials= Credentials().credentials
playlist_obj = spotify_modules.Playlist(credentials, playlist_id)
playlist_name = playlist_obj.playlist_name
    
def get_silver_table(playlist_name):
    '''
    Returns the silver playlist Data Frame from Delta Lake.

            Parameters:
                    playlist_name (str) : playlist name from pipeline @variable('playlist_name')
            Returns:
                    silver_df (spark.DataFrame): Audio data for playlist spark.DataFrame
    '''
    path = Routing('silver', playlist_name).path
    silver_df = spark.read.format('delta').load(path)
    return silver_df

def save_fact_table(silver_df, playlist_name):
    '''
    Returns the fact table spark.DataFrame from Playlist.

            Parameters:
                    silver_df (spark.DataFrame): Audio data for playlist spark.DataFrame
                    playlist_name (str) : playlist name from pipeline @variable('playlist_name')
            Returns:
                    fact_df (spark.DataFrame): Audio data for playlist spark.DataFrame for fact columns only.
    '''
    fact_columns = ['track_id', 'popularity', 'danceability', 'energy',
     'key', 'loudness', 'mode', 'speechiness', 'acousticness',
     'instrumentalness', 'liveness', 'valence', 'tempo', 'duration',
     'end_of_fade_in', 'start_of_fade_out', 'tempo_confidence',
     'time_signature_confidence', 'key_confidence', 'mode_confidence', 'artist_id', 'album_id']

    path = Routing('gold', f'{playlist_name}//fact_data').path
    fact_df = silver_df.select(fact_columns)
    fact_df.write.mode('overwrite').format('delta').save(path)
    fact_df.display()
    return fact_df

silver_df = get_silver_table(playlist_name)
fact_df = save_fact_table(silver_df, playlist_name)

# COMMAND ----------

# Refactoring
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def make_artists_df(silver_df):
    '''
    Returns Artists data with id for many artists in one track.

            Parameters:
                    silver_df (spark.DataFrame): Audio data for playlist spark.DataFrame
            Returns:
                    df_artists (spark.DataFrame): Artists data with id for many artists in one track.
    '''
    w = Window.orderBy(F.lit(1))
    df_artists = (
        silver_df
          .select(['artist_name', 'artist_id'])
          .withColumn('id', F.row_number().over(w))
          .withColumn('artist_true_ids_array', F.col('artist_id'))
          .drop('artist_id')
          .select('id', 'artist_name', 'artist_true_ids_array')
    )
    return df_artists

def explode_artists_n_ids_cols(df_artists):
    '''
    Returns exploded one-column spark.DataFrame -> artist_true_ids_array, artist_name.

            Parameters:
                    df_artists (spark.DataFrame): Artists data with id for many artists in one track.
            Returns:
                    true_ids_column (spark.DataFrame): Exploded true (single) artists ids.
                    artists_name_column (spark.DataFrame): Exploded true (single) artists names.
    '''
    true_ids_column = (
        df_artists
            .select('artist_true_ids_array')
            .withColumn('artist_true_ids_array', F.explode(F.col('artist_true_ids_array')))
            .select('artist_true_ids_array')
    )

    artists_name_column = (
        df_artists
            .select('artist_name')
            .withColumn('artist_name', F.explode(F.col('artist_name')))
    )
    return true_ids_column, artists_name_column

df_artists = make_artists_df(silver_df)
true_ids_column, artists_name_column = explode_artists_n_ids_cols(df_artists)

def drop_names_tracks_duplicates(true_ids_column, artists_name_column):
    '''
    Returns spark.DataFrame with exploded artist_name and id data without duplicates.

            Parameters:
                    true_ids_column (spark.DataFrame): Exploded true (single) artists ids.
                    artists_name_column (spark.DataFrame): Exploded true (single) artists names.
            Returns:
                    artists_names_df (spark.DataFrame): Exploded Artists data without duplicates.
    '''
    artists_names_df = artists_name_column.toPandas()
    artists_names_df[id] = true_ids_column.toPandas()
    artists_names_df = artists_names_df.drop_duplicates()
    artists_names_df.columns = ['artist_name', 'id']
    artists_names_df = spark.createDataFrame(artists_names_df)
    return artists_names_df

artists_names_df = drop_names_tracks_duplicates(true_ids_column, artists_name_column)

def save_artists_dim_table(artists_names_df, playlist_name):
    '''
    Saves ready spark.DataFrame with exploded artist_name and id data without duplicates in Delta Lake.

            Parameters:
                    artists_names_df (spark.DataFrame): Exploded Artists data without duplicates.
                    playlist_name (str) : playlist name from pipeline @variable('playlist_name')
            Returns:
                    artists_names_df (spark.DataFrame): Exploded Artists data without duplicates.
    '''
    path = Routing('gold', f'{playlist_name}//dim_artists').path
    artists_names_df.select('id', 'artist_name').write.mode('overwrite').save(path)
    return artists_names_df
                           
artists_dim_table = save_artists_dim_table(artists_names_df, playlist_name)
