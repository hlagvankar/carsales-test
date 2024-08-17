import argparse

from transform import transform_data
from load import load_data
from utils import *

def main(input_dir, output_dir):
    spark = create_spark_session()
    
    # Load the data
    accounts_df, skus_df, invoices_df, invoice_line_items_df = load_data(spark, input_dir)
    
    # Transform the data
    account_features_df = transform_data(accounts_df, skus_df, invoices_df, invoice_line_items_df)
    
    # Save the transformed data
    write_output(account_features_df, output_dir)
    
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL Pipeline for processing CSV data')
    parser.add_argument('--input_dir', required=True, help='Input directory containing CSV files')
    parser.add_argument('--output_dir', required=True, help='Output directory for processed data')
    
    args = parser.parse_args()
    
    main(args.input_dir, args.output_dir)    