import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count
from datetime import datetime

# Initialize Logging
LOG_FILE = "financial_risk_processing.log"
logging.basicConfig(level=logging.INFO, filename=LOG_FILE, filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FinancialRiskProcessing") \
    .getOrCreate()

# S3 paths
RAW_S3_PATH = "s3://spark-project-raw-bucket/exported_financial_risk.csv"
CLEAN_S3_PATH = "s3://spark-project-prod-bucket/processed_data/cleaned_financial_risk.parquet"
AGG_S3_PATH = "s3://spark-project-prod-bucket/aggregate_data/aggregated_financial_risk.parquet"
LOG_S3_PATH = "s3://my-allproject-logs/financial-risk-logs/financial_risk_processing.log"

try:
    logging.info("Starting data processing pipeline")
    
    # Read raw data
    df = spark.read.csv(RAW_S3_PATH, header=True, inferSchema=True)
    logging.info(f"Raw data read successfully with {df.count()} records")

    # Data Cleaning
    df_cleaned = df.fillna({
        "Gender": "Unknown",
        "Employment_Status": "Unemployed",
        "Credit_Score": df.agg(avg("Credit_Score")).collect()[0][0],
        "Income": df.agg(avg("Income")).collect()[0][0],
        "City": "Unknown",
        "State": "Unknown",
        "Zipcode": "00000"
    })

    df_cleaned = df_cleaned.withColumn("Credit_Score", col("Credit_Score").cast("int")) \
                           .withColumn("Income", col("Income").cast("double"))
    logging.info(f"Data cleaning completed with {df_cleaned.count()} records")

    # Aggregations
    df_aggregated = df_cleaned.groupBy("Gender", "Employment_Status", "City", "State", "Zipcode") \
        .agg(
            avg("Credit_Score").alias("Avg_Credit_Score"),
            avg("Income").alias("Avg_Income"),
            count("Credit_Score").alias("Total_Records")
        )
    logging.info("Data aggregation completed")

    # Save cleaned and aggregated data back to S3
    df_cleaned.write.mode("overwrite").parquet(CLEAN_S3_PATH)
    df_aggregated.write.mode("overwrite").parquet(AGG_S3_PATH)
    logging.info("Data successfully written to S3")

except Exception as e:
    logging.error(f"Error during processing: {str(e)}")

finally:
    spark.stop()
    logging.info("Spark session stopped")
    
    # Upload log file to S3
    s3_client = boto3.client('s3')
    s3_client.upload_file(LOG_FILE, LOG_S3_PATH, f"financial_risk_processing_{datetime.now().strftime('%Y%m%d%H%M%S')}.log")
    logging.info("Log file uploaded to S3")
