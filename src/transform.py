from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum
from pyspark.sql.types import IntegerType, DateType

def transform_data(accounts, invoices, invoice_line_items, skus):
    # Join invoice_line_items with skus to get product details
    invoice_items_with_sku = invoice_line_items.join(skus, "item_id")
    
    # Calculate total cost per invoice
    invoice_totals = invoice_items_with_sku.groupBy("invoice_id") \
                        .agg(spark_sum("item_cost_price").alias("total_cost"))
    
    # Join with invoices to get full invoice data
    invoices = invoices.join(invoice_totals, "invoice_id") \
                        .withColumnRenamed("date_issued", "invoice_date")
    
    # Join with accounts to get customer details
    final_df = invoices.join(accounts, "account_id")
    
    return final_df