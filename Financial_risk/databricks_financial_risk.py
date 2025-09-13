# Databricks notebook source
from pyspark.sql.functions import when, col, lower, trim, row_number
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
import boto3

# Note: SparkSession is automatically created in Databricks as 'spark'
print("Spark is ready:", spark.version)


# Data read - Update path to your Databricks file location
# Option 1: If file is uploaded to DBFS FileStore
df = spark.read.csv('/FileStore/shared_uploads/financial_risk.csv', header=True, inferSchema=True)

# Create ID column using window function
window_spec = Window.orderBy("age")
df_main = df.withColumn("id", row_number().over(window_spec))

# Column renaming and selection
df_main_1 = df_main.select(
    col("id"),
    col("Gender"),
    col("Education level").alias("Qualification"),
    col("Marital Status").alias("Marital_status"),
    col("Income"),
    col("Credit Score").alias("Credit_score"),
    col("Loan Amount").alias("Loan_Amount"),
    col("Loan Purpose").alias("Loan_Purpose"),
    col("Employment Status").alias("Employment_Status"),
    col("Years at Current Job").alias("Current_Job_YOE"),
    col("Payment History").alias("Payment_History"),
    col("Debt-to-Income Ratio").alias("Debt_to_Income_Ratio"),
    col("Assets Value").alias("Assets_Value"),
    col("Number of Dependents").alias("Dependents"),
    col("City"),
    col("State"),
    col("Country"),
    col("Previous Defaults").alias("Previous_Defaults"),
    col("Risk Rating").alias("Risk_Rating")
)

# Data exploration
df_gender = df_main_1.select('Gender').groupBy('Gender').count()
display(df_gender) 

# Create temporary view for SQL queries
df_main_1.createOrReplaceTempView('financial_risk_table')

# SQL query 
df_loanPurpose = spark.sql("SELECT Loan_Purpose, COUNT(1) as count FROM financial_risk_table GROUP BY Loan_Purpose")
display(df_loanPurpose)

df_Qualification = spark.sql("SELECT qualification, COUNT(1) as cnt FROM financial_risk_table GROUP BY qualification")
display(df_Qualification)

# Data transformations
df_qualification_altered = df_main_1.withColumn(
    "Qualification",
    when(lower(trim(col("Qualification"))).like("%bachelor's%"), "UG")
    .when(lower(trim(col("Qualification"))).like("%master's%"), "PG")
    .when(lower(trim(col("Qualification"))).like("%high school%"), "HS")
    .otherwise(col("Qualification"))
)

# Handle null values
df_income = df_qualification_altered.fillna(0.0, subset=['Income'])
df_not_nulls = df_income.fillna(0.0, subset=['Loan_Amount','Credit_Score','Dependents','Previous_Defaults'])

# Data type casting
df_main_final_1 = df_not_nulls \
    .withColumn("Dependents", col("Dependents").cast("int")) \
    .withColumn("Previous_Defaults", col("Previous_Defaults").cast("int"))

df_main_final_2 = df_main_final_1.withColumn('Credit_score', col('Credit_score').cast('int'))

# Filter out invalid data
df_dropped_junk = df_main_final_2.filter(
    (col("Income") != 0) & (col("Credit_score") != 0)
)

# Cast to decimal types
df_main_3 = df_dropped_junk.withColumn("Debt_to_Income_Ratio", col("Debt_to_Income_Ratio").cast(DecimalType(10, 3))) \
                            .withColumn('Income', col('Income').cast(DecimalType(10,3))) \
                            .withColumn('Loan_Amount', col('Loan_Amount').cast(DecimalType(10,3))) \
                            .withColumn('Assets_Value', col('Assets_Value').cast(DecimalType(10,3)))

df_final = df_main_3.fillna(0.000, subset=['Assets_Value'])

# Display final results
display(df_final)
print(f"Final dataset shape: {df_final.count()} rows, {len(df_final.columns)} columns")

# Option 1: Direct S3 write (recommended for Databricks)
df_final.write.mode("overwrite").parquet("s3a://testing-purpose-for-projects/sample/")

# Option 2: Write to DBFS first (alternative approach)
# df_final.write.mode("overwrite").parquet("dbfs:/FileStore/output/financial_risk_processed/")

print("Data processing and S3 write completed successfully!")