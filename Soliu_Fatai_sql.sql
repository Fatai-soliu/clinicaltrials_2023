-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("FileStore/tables")

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS bdtt_task

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file = "/FileStore/tables/clinicaltrial_2023/clinicaltrial_2023.csv"
-- MAGIC
-- MAGIC dbutils.fs.head(file)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create an rdd for clinicaltrial dataset
-- MAGIC  
-- MAGIC rdd1 = sc.textFile(file)
-- MAGIC
-- MAGIC rdd1.take(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Clean the rdd and convert to dataframe
-- MAGIC
-- MAGIC def eliminate_double_quotes(resp):
-- MAGIC     return [i.replace('"', '') for i in resp]
-- MAGIC
-- MAGIC # convert date to same format
-- MAGIC def convert_dates(date):
-- MAGIC     date[12] = date[12] + '-01' if len(date[12]) == 7 else date[12]
-- MAGIC     date[13] = date[13] + '-01' if len(date[13]) == 7 else date[13]
-- MAGIC     return date
-- MAGIC
-- MAGIC def populate_with_strings(item, max_fig):
-- MAGIC     return item + [''] * (max_fig - len(item))
-- MAGIC
-- MAGIC clinicaltrial_2023_rdd = rdd1. \
-- MAGIC                          map(lambda y: y.replace(',', '')). \
-- MAGIC                          map(lambda y: y.split('\t')). \
-- MAGIC                          map(eliminate_double_quotes). \
-- MAGIC                          map(lambda y: populate_with_strings(y, 14)). \
-- MAGIC                          filter(lambda y: y[0] != 'Id'). \
-- MAGIC                          map(convert_dates)
-- MAGIC
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC mySchema = StructType ([
-- MAGIC             StructField("Id", StringType()),
-- MAGIC             StructField("Study_Title", StringType()),
-- MAGIC             StructField("Acronynm", StringType()),
-- MAGIC             StructField("Status", StringType()),
-- MAGIC             StructField("Conditions", StringType()),
-- MAGIC             StructField("Interventions", StringType()),
-- MAGIC             StructField("Sponsor", StringType()),
-- MAGIC             StructField("Collaborators", StringType()),
-- MAGIC             StructField("Enrollment", StringType()),
-- MAGIC             StructField("Funder_Type", StringType()),
-- MAGIC             StructField("Type", StringType()),
-- MAGIC             StructField("Study_Design", StringType()),
-- MAGIC             StructField("Start", StringType()),
-- MAGIC             StructField("Completion", StringType()),
-- MAGIC ])
-- MAGIC
-- MAGIC df = spark.createDataFrame(clinicaltrial_2023_rdd , mySchema)
-- MAGIC
-- MAGIC # Change datetype 
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC df = df.withColumn("Enrollment", df.Enrollment.cast("int"))
-- MAGIC
-- MAGIC df = df.withColumn("Start", to_date(df["Start"], "yyyy-MM-dd"))
-- MAGIC df = df.withColumn("Completion", to_date(df["Completion"], "yyyy-MM-dd"))
-- MAGIC
-- MAGIC df.show(3)
-- MAGIC df.printSchema()

-- COMMAND ----------

-- DBTITLE 1,Create Pharma - Data cleaning
-- MAGIC %python
-- MAGIC # Create an rdd for pharma dataset
-- MAGIC
-- MAGIC pharma_file = "/FileStore/tables/pharma/pharma.csv"
-- MAGIC
-- MAGIC pharma_rdd = sc.textFile(pharma_file)
-- MAGIC
-- MAGIC # Create an rdd of pharmaceutical companies and convert to df
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC
-- MAGIC pharma_com = pharma_rdd \
-- MAGIC     .flatMap(lambda x: x.split('\n')) \
-- MAGIC     .filter(lambda line: not line.startswith('"Company')) \
-- MAGIC     .map(lambda line: line.split(",")) \
-- MAGIC     .map(lambda x: (x[1][1:-1])) \
-- MAGIC     .filter(lambda x: x[0].strip() != "") \
-- MAGIC     .filter(lambda x: x[0] != '')
-- MAGIC
-- MAGIC
-- MAGIC pharma_df = spark.createDataFrame(pharma_com, StringType())
-- MAGIC
-- MAGIC pharma_df.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Create SQL Table
-- MAGIC
-- MAGIC df.createOrReplaceTempView("clinicaltrial_2023_sql")
-- MAGIC
-- MAGIC pharma_df.createOrReplaceTempView("pharma_sql")

-- COMMAND ----------

-- SELECT * FROM clinicaltrial_2023_sql LIMIT 5

-- SELECT * FROM pharma_sql LIMIT 5

-- COMMAND ----------

-- Create table in the newly created database

CREATE OR REPLACE TABLE bdtt_task.clinicaltrial2023 AS SELECT * FROM clinicaltrial_2023_sql

-- COMMAND ----------

-- Create pharma table in the newly created database

CREATE OR REPLACE TABLE bdtt_task.pharma AS SELECT * FROM pharma_sql

-- COMMAND ----------

SELECT * FROM bdtt_task.clinicaltrial2023 LIMIT 5

-- COMMAND ----------

SELECT * FROM bdtt_task.pharma LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Question 1: Number of Distinct study
SELECT COUNT (Id) AS Number_of_study
FROM bdtt_task.clinicaltrial2023

-- COMMAND ----------

-- DBTITLE 1,Question 2: Types of studies and frequencies
SELECT Type, COUNT(Id) AS count
FROM bdtt_task.clinicaltrial2023
GROUP BY 1
ORDER BY 2 DESC

-- COMMAND ----------

-- DBTITLE 1,Question 3: Top 5 Conditions and frequencies
SELECT Conditions, COUNT(Id) AS count
FROM bdtt_task.clinicaltrial2023
GROUP BY 1
ORDER BY 2 DESC LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Question 4: 10 most common non-pharma company

WITH Top_10_sponsor AS (
    SELECT c.Sponsor, COUNT(Id) AS count
    FROM bdtt_task.clinicaltrial2023 c
    LEFT JOIN bdtt_task.pharma p ON c.Sponsor = p.value
    WHERE p.value IS NULL
    GROUP BY c.Sponsor
    ORDER BY count DESC
    LIMIT 10
)
SELECT *
FROM Top_10_sponsor;

-- COMMAND ----------

-- DBTITLE 1,Question 5: Number of completed Studies for each month in 2023
-- Writing CTE to get completed studies for each month in 2023

WITH completion_date AS (
    SELECT Completion, Id
    FROM bdtt_task.clinicaltrial2023
    WHERE SUBSTRING(Completion, 1, 4) = '2023'
),
completion_month AS (
    SELECT *, MONTH(Completion) AS Month
    FROM completion_date
),
monthly_study AS (
    SELECT Month, COUNT(Id) AS count
    FROM completion_month
    GROUP BY Month
)
SELECT *
FROM monthly_study
ORDER BY Month;

-- COMMAND ----------


