from pyspark.sql.functions import col, lit, sum as spark_sum, when, datediff, current_date

def transform_data(accounts, invoices, invoice_line_items, skus):    
    # Join invoice_line_items with skus to get product details
    invoice_items_with_sku = invoice_line_items.join(skus, "item_id")
    
    print(f"Invoice Items With SKU")
    print(invoice_items_with_sku.show())

    # Calculate total cost per invoice
    invoice_totals = invoice_items_with_sku.groupBy("invoice_id") \
                        .agg(spark_sum("item_cost_price").alias("total_cost"))

    print(f"Invoices Totals")
    print(invoice_totals.show())

    # Cast date_issued to date
    invoices = invoices.withColumn("date_issued", col("date_issued").cast("date"))

    print(f"Cleaned Invoices")
    print(invoices.show())

    # Assume a standard payment term of 30 days
    payment_due_days = 28

    # Calculate the number of days since the invoice was issued
    invoices = invoices.withColumn(
        "days_since_issued", datediff(current_date(), col("date_issued"))
    )

    print(f"number of days since the invoice was issued")
    print(invoices.show())

    # Determine if an invoice is late
    invoices = invoices.withColumn(
        "is_late", when(col("days_since_issued") > payment_due_days, 1).otherwise(0)
    )

    print(f"Check if invoice is late")
    print(invoices.show())

    # Aggregate late payment information by account
    late_payments_summary_df = invoices.groupBy("account_id").agg(
        spark_sum("is_late").alias("total_late_invoices"),
        spark_sum("days_since_issued").alias("total_days_since_issue")
    )

    print(f"Aggregate late payment information by account")
    print(late_payments_summary_df.show())
    
    # Join with accounts to get customer details
    final_df = invoices.join(late_payments_summary_df, "account_id")
    print("Final Result")
    print(final_df.show())
    
    return final_df
