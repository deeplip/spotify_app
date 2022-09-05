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
path = Routing('silver', playlist_name).path
df = spark.read.format('delta').load(path)

# COMMAND ----------

fact_columns = ['track_id', 'popularity', 'danceability', 'energy',
 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration',
 'end_of_fade_in', 'start_of_fade_out', 'tempo_confidence',
 'time_signature_confidence', 'key_confidence', 'mode_confidence', 'artist_id', 'album_id']

path = path = Routing('gold', f'{playlist_name}//fact_data').path
df.select(fact_columns).write.mode('overwrite').format('delta').save(path)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.lit(1))

df_artists = (
    df
      .select(['artist_name', 'artist_id'])
      .withColumn('id', F.row_number().over(w))
      .withColumn('artist_true_ids_array', F.col('artist_id'))
      .drop('artist_id')
      .select('id', 'artist_name', 'artist_true_ids_array')
)

# COMMAND ----------

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

# COMMAND ----------

artists_names_df = artists_name_column.toPandas()
artists_names_df[id] = true_ids_column.toPandas()
artists_names_df = artists_names_df.drop_duplicates()
artists_names_df.columns = ['artist_name', 'id']
artists_names_df = spark.createDataFrame(artists_names_df)
path = Routing('gold', f'{playlist_name}//dim_artists').path
artists_names_df.select('id', 'artist_name').write.mode('overwrite').save(path)
