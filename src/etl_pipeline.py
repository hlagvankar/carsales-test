import argparse
import logging
import sys

from transform import transform_data
from load import load_data
from utils import *

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(input_dir, output_dir):
    spark = None
    try:
        spark = create_spark_session(logger)
        
        # Load the data
        accounts_df, skus_df, invoices_df, invoice_line_items_df = load_data(spark, input_dir, logger)

        logger.info(accounts_df.show())
        logger.info(skus_df.show())
        logger.info(invoices_df.show())
        logger.info(invoice_line_items_df.show())
        
        # Transform the data
        account_features_df = transform_data(accounts_df, invoices_df, invoice_line_items_df, skus_df, logger)
        
        # Save the transformed data
        logger.info(f"Writing output to {output_dir} directory in CSV and Parquet formats...")
        write_output(account_features_df, output_dir, logger)
    except Exception as e:
        logger.critical(f"ETL pipeline failed: {e}")
        sys.exit(1)    
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL Pipeline for processing CSV data')
    parser.add_argument('--input_dir', required=True, help='Input directory containing CSV files')
    parser.add_argument('--output_dir', required=True, help='Output directory for processed data')
    
    args = parser.parse_args()
    logger.info(f"Using Input Dir - ${args.input_dir}")
    logger.info(f"Using Output Dir - ${args.output_dir}")

    main(args.input_dir, args.output_dir)
    