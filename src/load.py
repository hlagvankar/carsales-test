import os

def load_data(spark, input_dir):
    accounts_df = spark.read.csv(os.path.join(input_dir, 'accounts.csv'), header=True, inferSchema=True)
    skus_df = spark.read.csv(os.path.join(input_dir, 'skus.csv'), header=True, inferSchema=True)
    invoices_df = spark.read.csv(os.path.join(input_dir, 'invoices.csv'), header=True, inferSchema=True)
    invoice_line_items_df = spark.read.csv(os.path.join(input_dir, 'invoice_line_items.csv'), header=True, inferSchema=True)
    
    return accounts_df, skus_df, invoices_df, invoice_line_items_df