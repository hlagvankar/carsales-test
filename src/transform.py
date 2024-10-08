from pyspark.sql.functions import col, lit, sum as spark_sum, when, datediff, current_date, regexp_replace, collect_set, concat_ws

def perform_sanity_checks(accounts, invoices, invoice_line_items, skus, logger):
    # Check if we have expected columns in respective DFs
    expected_columns = {
        'accounts': ['account_id', 'company_name', 'company_address','contact_person','contact_phone','gender','joining_date'],
        'invoices': ['invoice_id', 'account_id', 'date_issued'],
        'invoice_line_items': ['invoice_id', 'item_id', 'quantity'],
        'skus': ['item_id', 'item_name', 'item_cost_price', 'item_description', 'item_retail_price']
    }

    for df, name in zip([accounts, invoices, invoice_line_items, skus], expected_columns.keys()):
        missing_columns = set(expected_columns[name]) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing columns in {name}: {missing_columns}")
            raise ValueError(f"Missing columns in {name}: {missing_columns}")

def clean_company_address(accounts):
    # Clean Accounts DF for company address appearing on multiline
    accounts = accounts.withColumn("cleaned_company_address", regexp_replace("company_address", r"[\r\n]+", " ")) \
                        .drop("company_address") \
                        .withColumnRenamed("cleaned_company_address", "company_address")
    return accounts

def get_late_payment_data(accounts, invoices, invoice_line_items, skus, logger):
    try:

        # Join invoice_line_items with skus to get product details
        invoice_items_with_sku = invoice_line_items.join(skus, "item_id")
        
        logger.info(f"Invoice Items With SKU")
        logger.info(invoice_items_with_sku.show())

        # Calculate total cost per invoice
        invoice_totals = invoice_items_with_sku.groupBy("invoice_id") \
                            .agg(spark_sum("item_cost_price").alias("total_cost"))
        
        logger.info(f"Invoices Totals")
        logger.info(invoice_totals.show())

        # Cast date_issued to date
        invoices = invoices.withColumn("date_issued", col("date_issued").cast("date"))

        logger.info(f"Cleaned Invoices")
        logger.info(invoices.show())

        # Assume a standard payment term of 30 days
        payment_due_days = 30

        # Calculate the number of days since the invoice was issued
        invoices = invoices.withColumn(
            "days_since_issued", datediff(current_date(), col("date_issued"))
        )

        logger.info(f"number of days since the invoice was issued")
        logger.info(invoices.show())

        # Determine if an invoice is late
        invoices = invoices.withColumn(
            "is_late", when(col("days_since_issued") > payment_due_days, 1).otherwise(0)
        )

        logger.info(f"Check if invoice is late")
        logger.info(invoices.show())

        # Aggregate late payment information by account
        late_payments_summary_df = invoices.groupBy("account_id").agg(
            spark_sum("is_late").alias("total_late_invoices"),
            spark_sum("days_since_issued").alias("total_days_since_issue")
        )

        logger.info(f"Aggregate late payment information by account")
        logger.info(late_payments_summary_df.show())

        # Clean Accounts DF for company address appearing on multiline
        accounts = clean_company_address(accounts)
        
        # Join with accounts to get customer details
        final_df = late_payments_summary_df.join(accounts, "account_id")
        logger.info("Final Result")
        logger.info(final_df.show())
        
        return final_df
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def get_skus_for_accounts(accounts, invoices, invoice_line_items, skus, logger):
    try:
                      
        # Join invoice_line_items with skus to get product details
        invoice_items_with_sku_and_accounts = invoice_line_items.join(skus, "item_id") \
                                                            .join(invoices, "invoice_id") \
                                                            .join(accounts, "account_id")
                                                            
        
        logger.info(f"Invoice Items With SKU and Accounts")
        logger.info(invoice_items_with_sku_and_accounts.show())

        sku_types_by_account = invoice_items_with_sku_and_accounts.groupBy("account_id", "company_name") \
                                .agg(collect_set("item_name").alias("sku_types_purchased"))
        
        product_mix = sku_types_by_account.withColumn("sku_types_purchased_str", concat_ws(",", col("sku_types_purchased"))) \
                                         .drop("sku_types_purchased") \
                                         .withColumnRenamed("sku_types_purchased_str", "sku_types_purchased")

        logger.info(f"SKUs Types By Account")        
        logger.info(product_mix.show())

        return product_mix
    except Exception as e:
        logger.error(f"Failed to get SKUs by each account. ERROR: {e}")
        raise