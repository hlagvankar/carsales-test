from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("Carsales ETL Pipeline") \
        .getOrCreate()

def read_csv(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def write_output(df, output_dir, format='parquet'):
    if format == 'csv':
        df.write.mode("overwrite").csv(output_dir)
    elif format == 'parquet':
        df.write.mode("overwrite").parquet(output_dir)
    else:
        print(f"Unknown output file format specified. {format}")