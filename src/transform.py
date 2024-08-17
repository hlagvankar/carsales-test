from pyspark.sql.functions import col, datediff, current_date, sum, avg

def transform_data(accounts_df, skus_df, invoices_df, invoice_line_items_df):
    # Join the DataFrames
    invoice_details_df = invoices_df.join(invoice_line_items_df, 'invoice_id') \
                                    .join(accounts_df, 'account_id') \
                                    .join(skus_df, 'sku_id')

    # Calculate days past due
    invoice_details_df = invoice_details_df.withColumn(
        'days_past_due', datediff(current_date(), col('due_date'))
    )

    # Aggregate data by account
    account_features_df = invoice_details_df.groupBy('account_id').agg(
        sum('amount_due').alias('total_outstanding_balance'),
        avg('days_past_due').alias('avg_payment_delay')
    )
    
    return account_features_df