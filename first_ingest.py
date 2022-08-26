# Databricks notebook source
import spotify_modules
import pandas as pd

playlist = '37i9dQZF1DXcBWIGoYBM5M' # PARAM

def get_playlist(playlist_id):
    playlist_obj = spotify_modules.Playlist(playlist)
    playlist_df = pd.DataFrame({'track_id':playlist_obj.track_id,'artist_name' : playlist_obj.artists_names,'track_name' : playlist_obj.tracks_names, 'popularity':playlist_obj.popularity, 'artist_id' : playlist_obj.artists_ids,  'album_id' : playlist_obj.album_id})
    return playlist_df

def get_audio_data(playlist_df):
    tracks_data = playlist_df['track_id'].map(spotify_modules.Track)
    audio_analysis_df = pd.DataFrame([track.audio_analysis for track in tracks_data])
    audio_features_df = pd.DataFrame([track.audio_features for track in tracks_data])
    audio_df = pd.concat([audio_features_df, audio_analysis_df], axis = 1)
    data_df = pd.merge(playlist_df, audio_df, left_on='track_id', right_on='id')
    return data_df
