from inspect import Attribute
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

class Auth:
    def __init__(self):
        self.secrets = self.get_secrets()
        self.__id = self.secrets[0]
        self.__secret = self.secrets[1]
        self.__credentials = SpotifyClientCredentials(
            client_id=self.__id, client_secret=self.__secret)
        self.spotify = spotipy.Spotify(auth_manager=self.__credentials)

    def get_secrets(self):
        with open("secrets.txt", "r") as txt_file:
            secrets = txt_file.readlines()[0]
            secrets = secrets.split(',')
            return secrets


class Playlist(Auth):
    def __init__(self, playlist_id):
        super().__init__()
        print(self.secrets[0])
        self.playlist_id = playlist_id
        self.track_objects = self.spotify.playlist(self.playlist_id)[
            'tracks']['items']
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
        if playlist_df is not None:
            playlist_df = self.get_df()
        tracks_data = playlist_df['track_id'].map(Track)
        common_cols = ['key', 'tempo', 'loudness', 'mode', 'time_signature']
        audio_analysis_df = pd.DataFrame([track.audio_analysis for track in tracks_data]).drop(common_cols, axis = 1)
        audio_features_df = pd.DataFrame([track.audio_features for track in tracks_data])
        audio_df = pd.concat([audio_features_df, audio_analysis_df], axis = 1)
        data_df = pd.merge(playlist_df, audio_df, left_on='track_id', right_on='id')
        return data_df
        
class Track(Auth):
    def __init__(self, track_id):
        super().__init__()
        self.track_id = track_id
        self.track_object = self.spotify.track(self.track_id)
        self.audio_features = self.spotify.audio_features(self.track_id)[0]
        self.audio_analysis = self.spotify.audio_analysis(self.track_id)[
            'track']

class Album(Auth):
    def __init__(self, album_id):
        super().__init__()
        self.album_id = album_id
        self.data = self.spotify.album(self.album_id)


# class Artist(Auth):
#     def __init__(self, artist_id):
#     super().__init__()
#     self.album_id = album_id
        
    
        