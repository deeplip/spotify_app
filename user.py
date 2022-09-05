# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS users
# MAGIC   LOCATION 'abfss://users@spotify0storage.dfs.core.windows.net/';
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS users.users_parquet (
# MAGIC   username string,
# MAGIC   pwhash string
# MAGIC )
# MAGIC   USING PARQUET
# MAGIC     LOCATION 'abfss://users@spotify0storage.dfs.core.windows.net/';

# COMMAND ----------

def insert_user(username, password):
    import hashlib
    col1, col2 = 'username', 'pwhash'
    pwhash = hashlib.sha256(password.encode()).hexdigest()
    spark.sql(f'''
        INSERT INTO users.users_parquet({col1}, {col2}) 
            VALUES("{username}", "{pwhash}");
    ''')
    return true
    
def login(username, password):
    import hashlib
    pwhash = hashlib.sha256(password.encode()).hexdigest()
    users_list = spark.sql(f'''
        SELECT username FROM users.users_parquet
            WHERE pwhash == "{pwhash}"
    '''
        ).rdd.flatMap(lambda x: x).collect()
    return username in users_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create new user:

# COMMAND ----------

username = ''
password = ''
insert_user(username, password)
