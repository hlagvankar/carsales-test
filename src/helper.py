import os

def save_data(account_features_df, output_dir, output_format = 'parquet'):
    if output_format == "parquet":
        account_features_df.write.mode('overwrite').parquet(os.path.join(output_dir, 'account_features'))
    elif output_format == "csv":
        account_features_df.write.mode('overwrite').csv(os.path.join(output_dir, 'account_features'), header=True)
    else:
        print(f"Unknow output file format supplied. {output_format}")