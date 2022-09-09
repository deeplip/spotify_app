# Databricks notebook source
from modules import spotify_modules
from spotipy.oauth2 import SpotifyClientCredentials

class Credentials:
    def __init__(self):
        self.client_id = dbutils.secrets.get(scope = 'SpotifyClientID', key = 'SpotifyClientID')
        self.key = dbutils.secrets.get(scope = 'SpotifySecretKey', key = 'SpotifySecretKey')
        self.credentials = SpotifyClientCredentials(client_id= self.client_id, client_secret=self.key)
        
class Routing:
    def __init__(self, container_name, file_name, strg_name = 'spotify0storage'):
        self.container_name = container_name
        self.file_name = file_name
        self.path = f"abfss://{self.container_name}@{strg_name}.dfs.core.windows.net/{self.file_name}"
        
def get_param_widget(widget_name):
    dbutils.widgets.text(widget_name, "", "")
    return dbutils.widgets.get(widget_name)
