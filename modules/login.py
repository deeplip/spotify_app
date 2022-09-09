# Databricks notebook source
# MAGIC %run ../modules/spotify_utils

# COMMAND ----------

def login(username, password):
    import hashlib
    pwhash = hashlib.sha256(password.encode()).hexdigest()
    users_list = spark.sql(f'''
        SELECT username FROM users.users_parquet
            WHERE pwhash == "{pwhash}"
    '''
        ).rdd.flatMap(lambda x: x).collect()
    if username in users_list:
        params_table = 'config'
        path = Routing('params', params_table).path
        df = spark.read.format('parquet').load(path)
        return df.select('playlist_name').rdd.flatMap(lambda x: x).collect()
    raise Exception("Wrong username or password.")
