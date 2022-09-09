# Databricks notebook source
# MAGIC %run ../spotify_app/modules/login

# COMMAND ----------

username = 'test_login'
password = 'test_pw_1'

playlists_names = login(username, password)


dbutils.widgets.dropdown("playlist", playlists_names[0], playlists_names)
# df.write.format('csv').path(path)

# COMMAND ----------

# MAGIC %run ../spotify_app/modules/pipeline_trigger

# COMMAND ----------

main_param

# COMMAND ----------

main_param = dbutils.widgets.get("playlist")
pipeline_run(main_param) #sprawdzić jaki jest adres(wasb?) do bloba. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notebook do tworzenia kont
# MAGIC - podaj login i haslo
# MAGIC - wstaw do bazy danych kodując haslo
# MAGIC 
# MAGIC #### Notebook interface'owy
# MAGIC - secret_1 == secret_2
# MAGIC - podaj login i haslo, uzyskaj potwierdzenie z bazy Users
# MAGIC - Jeżeli credentials dobre daj z table praquet listę playlist
# MAGIC - wybierz z comboboxa odpowiednią playlistę
# MAGIC - zapisz plik konfiguracyjny na storage
# MAGIC - triggeruj kiedy pojawi się plik (dodac usuniecie pliku config albo zmiane nazwy na stary)
