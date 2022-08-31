from inspect import Attribute
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

class Auth:
    def __init__(self, credentials):
        self.__credentials = credentials
        self.spotify = spotipy.Spotify(auth_manager=self.__credentials)

class Playlist(Auth):
    def __init__(self, credentials, playlist_id):
        super().__init__(credentials)
        self.playlist_id = playlist_id
        self.playlist_obj = self.spotify.playlist(self.playlist_id)
        self.playlist_name = self.playlist_obj['name'].replace("'", "").replace(" ", "")
        self.track_objects = self.playlist_obj['tracks']['items']
        self.track_items = [item['track'] for item in self.track_objects]
        self.artists_objects = [item['artists'] for item in self.track_items]

    @property
    def tracks_names(self):
        return [item['name'] for item in self.track_items]

    @property
    def artists_names(self):
        return [[artist['name'] for artist in artists] for artists in self.artists_objects]

    @property
    def artists_ids(self):
        return [[artist['id'] for artist in artists] for artists in self.artists_objects]

    @property
    def track_id(self):
        return [item['id'] for item in self.track_items]

    @property
    def album_id(self):
        return [item['album']['id'] for item in self.track_items]

    @property
    def popularity(self):
        return [item['popularity'] for item in self.track_items]
    
    def get_df(self):
        playlist_df = pd.DataFrame({'track_id':self.track_id,'artist_name' : self.artists_names,
                                    'track_name' : self.tracks_names, 'popularity':self.popularity,
                                    'artist_id' : self.artists_ids,  'album_id' : self.album_id})
        return playlist_df

    def get_audio_data(self, playlist_df = None):
        if playlist_df is None:
            playlist_df = self.get_df()
        audio_analysis = playlist_df['track_id'].map(self.track_audio_analysis)
        audio_features = playlist_df['track_id'].map(self.track_audio_features)
        common_cols = ['key', 'tempo', 'loudness', 'mode', 'time_signature']
        audio_analysis_df = pd.DataFrame([track for track in audio_analysis]).drop(common_cols, axis = 1)
        audio_features_df = pd.DataFrame([track for track in audio_features])
        audio_df = pd.concat([audio_features_df, audio_analysis_df], axis = 1)
        data_df = pd.merge(playlist_df, audio_df, left_on='track_id', right_on='id')
        return data_df
        
    def track_audio_analysis(self, track_id):
        track_object = self.spotify.track(track_id)
        audio_analysis = self.spotify.audio_analysis(track_id)['track']
        return audio_analysis

    def track_audio_features(self, track_id):
        track_object = self.spotify.track(track_id)
        audio_features = self.spotify.audio_features(track_id)[0]
        return audio_features
    
class Album(Auth):
    def __init__(self, credentials, album_id):
        super().__init__(credentials)
        self.album_id = album_id
        self.data = self.spotify.album(self.album_id)

# class Artist(Auth):
#     def __init__(self, artist_id):
#     super().__init__()
#     self.album_id = album_id
        
    
        