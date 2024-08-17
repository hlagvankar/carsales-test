from pyspark.sql import SparkSession

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

def write_output(df, output_dir, logger):
    try:
        df.write.mode("overwrite").csv(output_dir, header=True)
        df.write.mode("overwrite").parquet(output_dir)
    except Exception as e:
        logger.error(f"Error writing data: {e}")
        raise