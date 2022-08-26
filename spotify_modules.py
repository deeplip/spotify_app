from inspect import Attribute
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


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
            secrets.split(',')
            return secrets


class Playlist(Auth):
    def __init__(self, playlist_id):
        super().__init__()
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
