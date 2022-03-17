# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring the Titanic Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data

# COMMAND ----------

# Define schema
from pyspark.sql.types import StructType, DoubleType, IntegerType, StringType

passenger_ticket_types = [('PassengerId',     StringType()),
                          ('Ticket',          StringType()),
                          ('Fare',            DoubleType()),
                          ('Cabin',           StringType()),
                          ('Embarked',        StringType()),
                          ('Pclass',          StringType()),
                          ('Parch',           StringType())]

passenger_demographic_types = [('PassengerId',StringType()),
                               ('Name',       StringType()),
                               ('Sex',        StringType()),
                               ('Age',        DoubleType()),
                               ('SibSp',      StringType())]

passenger_label_types = [('PassengerId',StringType()),
                         ('Survived',   IntegerType())]

# COMMAND ----------

from utils import create_tables

create_tables.convert_csv_to_delta(path="./data/passenger_demographics.csv", 
                                   col_types=passenger_demographic_types, 
                                   table_name="passenger_demographic_features")

create_tables.convert_csv_to_delta(path="./data/passenger_ticket.csv", 
                                   col_types=passenger_ticket_types, 
                                   table_name="passenger_ticket_features")

create_tables.convert_csv_to_delta(path="./data/passenger_labels.csv", 
                                   col_types=passenger_label_types, 
                                   table_name="passenger_labels")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploratory Data Analysis

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE
# MAGIC OR REPLACE VIEW titanic AS
# MAGIC SELECT
# MAGIC   t.PassengerId,
# MAGIC   Cabin,
# MAGIC   Fare,
# MAGIC   Pclass,
# MAGIC   Sex,
# MAGIC   Embarked,
# MAGIC   Parch,
# MAGIC   SibSp,
# MAGIC   Survived,
# MAGIC   Age
# MAGIC FROM
# MAGIC   passenger_ticket_features t
# MAGIC   LEFT JOIN passenger_demographic_features d ON d.PassengerId = t.PassengerId
# MAGIC   LEFT JOIN passenger_labels l ON l.PassengerId = t.PassengerId;
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   titanic

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean up missing values

# COMMAND ----------

# MAGIC %pip install bamboolib==1.30.0

# COMMAND ----------

import bamboolib as bam

bam
