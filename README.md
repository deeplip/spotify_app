# Spotify API pipeline on Azure Data Factory and Azure Databricks

[![Spotify API](https://developer.spotify.com/assets/WebAPI_intro.png)](https://developer.spotify.com/documentation/web-api/)
[![Databricks](https://scriptfactory.pl/blog/wp-content/uploads/2022/02/og-databricks.png)](https://azure.microsoft.com/en-us/free/databricks/)

### Introduction
Project has been developed only for learning purposes. In general, the idea was to make **Azure Data Factory pipeline** which:
- ingests data from **Spotify API** (Spotipy lib), 
- transforms, clean and structure data in **Databricks notebooks**,
- loads the data in to Databricks **Delta Lake**.

### Databricks catalogs structure
![Structure](https://i.postimg.cc/vH37hS9h/sctructure.png)
### Pipeline structure
![Pipeline](https://i.postimg.cc/y8wzJjf6/pipeline-ADF.png)
### Authentication
- For authentication Spotify credentials, which are: **Client ID** and **Secret**, Azure KeyVault has been used.
- For **Azure Storage Account** authentication Storage Kay has been added to Databricks cluster configuration.

### How it works
- Pipeline is triggered by dumping configuration .json file on ADLS gen2. That enables user to process data with different parameters (main parameter is playlist_id). You can run pipeline with any playlist_id which exists on Spotify platform. In order to run _storage trigger_ Azure requires **EventGrid** resource in Subscription where ADF has been deployed.  
- **LookForParams** _lookup activity_ reads .json config file which output is used in **setPlaylistName** and **setPlaylistId**. These are _set variable_ activities.
- Next, the pipeline has to find whether playlist already exists on Storage Account: 
    - **getPlaylistOnStorage** _get metadata activity_ returns childItems, that allows to get all directories in container. Directories are playlists names.
    - Json of existing playlists is stored in **onStorage** _set variable activity_
    - In order to get only playlists names array **forEachFolderName** _ForEach activity_ has been used simultaneously with **BronzeFoldersArr** _append variable activity_.
    - Finally, **getPlaylistExists** _set variable activity_ establishes bool value for following **If data exists** _If Condition activity_ in _Pipeline Expression Builder_.

    ```
    @equals(1, length(
            intersection(
                array(variables('playlist_name')), 
                    array(variables('on_storage_arr'))
            )
        )
    )
    ```
- **If data exists** _If Condition activity_ : 
    - **true** - _first_ingest notebook_ builds brand new Delta Lake table, with raw data  _(bronze level)_. It contains data from `spotify.Playlist() `, `spotify.audio_analysis()` and `spotify.audio_features()`.
    - **false** - _next_ingest notebook_ just compare fetched data with data which exists in Table. It appends only this tracks, that are not in Table.
    
![Table Bronze level](https://i.postimg.cc/GtRXHsF7/bronze.png)

- Next, _Transform notebook_ drops part of columns, that would probably be not necessary in analysis (kind of cleaning). Notebook saves new Delta Lake Table **fact_data**, which is now on level _silver_.
- _Table_generator_ notebook makes 2 tables. Fact table with playlist_id, foreign id keys and numeric data that is able for aggregation. It makes also dimension table **dim_artits** table with artists ids and names.
- Finally, _analyst_transform notebook_ saves **fact_data** as _CSV_ for BI purposes. It also makes some processing in artist_id column, which was list type so far.

![Table Gold level](https://i.postimg.cc/TP4wdV6x/gold.png)

![Table Gold level](https://i.postimg.cc/0jV984rc/artists.png)


Used Data Factory components:
> pipeline, datasets, linked services,

> set variables, append variable, GetMetadata,

> ForEach, If Condition, Azure Notebooks

Used Azure Resources:
> Databricks, Data Factory,

> Storage Account (ADLS gen2), Event Grid, KeyVault

###### Thank you for your attention, have a good day :-)
