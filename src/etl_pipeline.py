import argparse
import logging
import sys

from transform import *
from load import load_data
from utils import *


def main(input_dir, output_dir):
    spark = None
    logger = setup_logger()

    try:
        spark = create_spark_session(logger)
        
        # Load the data
        accounts_df, skus_df, invoices_df, invoice_line_items_df = load_data(spark, input_dir, logger)

        logger.info(accounts_df.show())
        logger.info(skus_df.show())
        logger.info(invoices_df.show())
        logger.info(invoice_line_items_df.show())
        
        # Perform sanity checks on Datasets before transforming
        perform_sanity_checks(accounts_df, invoices_df, invoice_line_items_df, skus_df, logger)
        # Transform the data
        late_payment_df = get_late_payment_data(accounts_df, invoices_df, invoice_line_items_df, skus_df, logger)

        # Get Types Of SKUs purchased by each account
        product_mix = get_skus_for_accounts(accounts_df, invoices_df, invoice_line_items_df, skus_df, logger)
        
        # Save the transformed data
        logger.info(f"Writing output to {output_dir} directory in CSV and Parquet formats...")
        write_output(late_payment_df, output_dir, "late_payment", logger)

        # Save product_mix data
        logger.info(f"Writing product_mix output to {output_dir} directory in CSV and Parquet formats...")
        write_output(product_mix, output_dir, "product_mix", logger)

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

    main(args.input_dir, args.output_dir)
    