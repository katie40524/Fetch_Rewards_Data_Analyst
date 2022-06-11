# Databricks notebook source
# MAGIC %md
# MAGIC ## Second: Write a query that directly answers a predetermined question from a business stakeholder

# COMMAND ----------

brands = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/ychang21@gmu.edu/brands.csv")
users = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/ychang21@gmu.edu/users.csv")
receipts = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/ychang21@gmu.edu/receipts.csv")

# COMMAND ----------

display(brands)

# COMMAND ----------

display(receipts)

receipts.columns

# COMMAND ----------

display(users)

# COMMAND ----------

# MAGIC %md
# MAGIC To execute SQL in PySpark, I create a temporary view of the receipt dataset

# COMMAND ----------

users.createOrReplaceTempView("users")

# COMMAND ----------

receipts.createOrReplaceTempView("receipts")

# COMMAND ----------

brands.createOrReplaceTempView("brands")

# COMMAND ----------

# MAGIC %md
# MAGIC What are the top 5 brands by receipts scanned for most recent month?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   rewardsReceiptItemList_brandCode,
# MAGIC   COUNT(_id) AS receipt_scanned_count,
# MAGIC   LEFT(dateScanned, 7) AS month
# MAGIC FROM receipts
# MAGIC WHERE 
# MAGIC   rewardsReceiptItemList_brandCode IS NOT NULL
# MAGIC   AND LEFT(dateScanned, 7) = '2021/01'
# MAGIC GROUP BY
# MAGIC   rewardsReceiptItemList_brandCode, month
# MAGIC ORDER BY
# MAGIC   COUNT(_id) DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC According to the result, we can find out that the top five brands for receipts scanned for most recent month are HY-VEE, PEPSI, KROGER, KLEENEX and BEN AND JERRYS.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Third: Evaluate Data Quality Issues in the Data Provided

# COMMAND ----------

# MAGIC %md
# MAGIC I found that the numbers of brands in receipts table and the numbers of brands in brands table don't match, so I use SQL to calculate the brand number of each table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT(rewardsReceiptItemList_brandCode))
# MAGIC FROM receipts
# MAGIC WHERE 
# MAGIC   rewardsReceiptItemList_brandCode IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT brandCode)
# MAGIC FROM brands;

# COMMAND ----------

# MAGIC %md
# MAGIC I join the receipts table and brands together and find that many brandCode in brands table don't match brandCode in receipts table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   brandCode
# MAGIC FROM 
# MAGIC   receipts
# MAGIC     LEFT JOIN brands
# MAGIC       ON receipts.rewardsReceiptItemList_brandCode = brands.brandCode

# COMMAND ----------

# MAGIC %md
# MAGIC Then, I find that there are duplicate columns in users column, so I use SQL to check the count of each Id. The result shows that there are duplicate values of some userid.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   _id,
# MAGIC  COUNT(*) AS count_duplicate
# MAGIC FROM users
# MAGIC GROUP BY
# MAGIC   _id
# MAGIC HAVING 
# MAGIC   COUNT(*) > 1
# MAGIC  ORDER BY
# MAGIC    count_duplicate DESC;
