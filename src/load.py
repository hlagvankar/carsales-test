import os
from utils import read_csv

def load_data(spark, input_dir):
    accounts_df = read_csv(spark, os.path.join(input_dir, 'accounts.csv'))
    skus_df = read_csv(spark, os.path.join(input_dir, 'skus.csv'))
    invoices_df = read_csv(spark, os.path.join(input_dir, 'invoices.csv'))
    invoice_line_items_df = read_csv(spark, os.path.join(input_dir, 'invoice_line_items.csv'))
    
    return accounts_df, skus_df, invoices_df, invoice_line_items_df