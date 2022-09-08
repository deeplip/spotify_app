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
# MAGIC -- USERNAME UNIQUE

# COMMAND ----------

def insert_user(username, password):
    import hashlib
    col1, col2 = 'username', 'pwhash' #To remove.
    pwhash = hashlib.sha256(password.encode()).hexdigest()
    spark.sql(f'''
        INSERT INTO users.users_parquet({col1}, {col2}) 
            VALUES("{username}", "{pwhash}");
    ''')
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create new user:

# COMMAND ----------

username = 'test_login'
password = 'test_pw_1'
insert_user(username, password)
