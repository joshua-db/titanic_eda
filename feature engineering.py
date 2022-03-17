# Databricks notebook source
# MAGIC %md
# MAGIC ### Install Feature Engineering Library

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Titanic Features

# COMMAND ----------

from featurelib import create_features

# COMMAND ----------

demographicDF = spark.sql("SELECT * FROM passenger_demographic_features")
ticketDF = spark.sql("SELECT * FROM passenger_ticket_features")

demographicFeaturesDF = create_features.compute_passenger_demographic_features(demographicDF)

ticketFeaturesDF = create_features.compute_passenger_ticket_features(ticketDF)

display(demographicFeaturesDF)

# COMMAND ----------

display(ticketFeaturesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Feature Store

# COMMAND ----------

# MAGIC %md
# MAGIC #### Demographics

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

feature_table_name = 'default.demographic_features'

# If the feature table has already been created, no need to recreate
try:
  fs.get_table(feature_table_name)
  print("Feature table entry already exists")
  pass
  
except Exception:
  fs.create_table(name = feature_table_name,
                          primary_keys = 'PassengerId',
                          schema = demographicFeaturesDF.schema,
                          description = 'Demographic-related features for Titanic passengers')

# COMMAND ----------

fs.write_table(
  
  name= feature_table_name,
  df = demographicFeaturesDF,
  mode = 'merge'

  )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tickets

# COMMAND ----------

feature_table_name = 'default.ticket_features'

# If the feature table has already been created, no need to recreate
try:
  fs.get_table(feature_table_name)
  print("Feature table entry already exists")
  pass
  
except Exception:
  fs.create_table(name = feature_table_name,
                          primary_keys = 'PassengerId',
                          schema = ticketFeaturesDF.schema,
                          description = 'Ticket-related features for Titanic passengers')

# COMMAND ----------

fs.write_table(
  
  name= feature_table_name,
  df = ticketFeaturesDF,
  mode = 'merge'
  
  )

# COMMAND ----------


