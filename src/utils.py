from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Carsales ETL Pipeline") \
        .getOrCreate()

def read_csv(spark, file_path):
    return spark.read.option("inferSchema", "true") \
                    .option("header", "true") \
                    .option("quote", '"') \
                    .option("escape", '"') \
                    .option("multiline", "true") \
                    .csv(file_path)

def write_output(df, output_dir):
    df.write.mode("overwrite").csv(output_dir, header=True)
    df.write.mode("overwrite").parquet(output_dir)
