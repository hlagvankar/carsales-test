import os
from utils import read_csv

def load_data(spark, input_dir, logger):
    try:
        accounts_df = read_csv(spark, os.path.join(input_dir, 'accounts.csv'))
        skus_df = read_csv(spark, os.path.join(input_dir, 'skus.csv'))
        invoices_df = read_csv(spark, os.path.join(input_dir, 'invoices.csv'))
        invoice_line_items_df = read_csv(spark, os.path.join(input_dir, 'invoice_line_items.csv'))
        
        if not all(os.path.exists(input_dir) for path in [os.path.join(input_dir, 'accounts.csv'), 
                                                          os.path.join(input_dir, 'skus.csv'), 
                                                          os.path.join(input_dir, 'invoices.csv'), 
                                                          os.path.join(input_dir, 'invoice_line_items.csv')]):
            raise FileNotFoundError("One or more input files are missing")
        
        # Validate loaded data
        if accounts_df is None or invoices_df is None or invoice_line_items_df is None or skus_df is None:
            raise ValueError("One or more input files failed to load.")
        
        logger.info("Data loaded successfully.")
        
        return accounts_df, skus_df, invoices_df, invoice_line_items_df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise