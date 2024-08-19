from pyspark.sql import SparkSession
import sys
import logging

def create_spark_session(logger):
    try:
        return SparkSession.builder \
            .appName("Carsales ETL Pipeline") \
            .getOrCreate()
    except Exception as e:
        logger.critical(f"Failed to create Spark Session: {e}")
        raise

def read_csv(spark, file_path):
    return spark.read.option("inferSchema", "true") \
                    .option("header", "true") \
                    .option("quote", '"') \
                    .option("escape", '"') \
                    .option("multiline", "true") \
                    .csv(file_path)

def write_output(df, output_dir, feature_type, logger):
    if df is None:
        logger.error(f"DataFrame for {feature_type} is None. Cannot write output.")
        raise ValueError(f"DataFrame for {feature_type} is None.")
    
    try:
        df.coalesce(1).write.mode("overwrite").csv(output_dir + f"/csv/{feature_type}", header=True)
        df.write.mode("overwrite").parquet(output_dir + f"/parquet/{feature_type}")
    except Exception as e:
        logger.error(f"Error writing data: {e}")
        raise

def setup_logger():
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger    