# Overview
This repository contains a Spark-based ETL pipeline designed to process raw data, extract useful features, and store the processed data for analytics purposes. The code is generic and reusable, allowing you to easily modify it to work with different datasets and transformations.  


# Problem Statement  
Enable business to predict late invoice payments

# Design Consideration and Scalability
* **Spark**: Spark is popular and widely used for its ability to handle large-scale data sets processing and its built-in support for distributed computing.  
* **Data Storage**: Parquet, again widely popular and efficient for querying datasets and provides good compression  
* **Incremental Processing**: Enhance the pipeline to process only new or changed data in each run, making it scalable for larger datasets  
* **Partitioning**: We can partition the output based on date or any key which is frequently used in Filter criteria while querying dataset.  
* **EMR**: We can use auto-scaling group to dynamically allocate resources based on workload.

# Intermediate Data Models
1. We can store raw datasets on S3 because of number of benefits S3 offers such as Durability, Cost Effectiveness, Scalability etc.
2. We can also store results from intermediate transformation on datasets for immediate access for business for analysis or query purpose. We can use either S3 or create Athena tables using Glue. If volume is large we can use, Redshift or any RDS on Aurora or Postgres on AWS which makes dataset available for querying with low-latency
3. To store the final output, we can again make use of S3, as S3 can be used as Data Lake and other systems can easiliy access files on S3 for their purpose

# Current Architecture
See image current_architecture.png

# Directory Structure
```bash 
carsales_etl/  
│  
├── data/  
|   |── accounts.csv  
│   ├── invoices.csv  
│   ├── invoice_line_items.csv  
│   └── skus.csv  
│  
├── src/  
│   ├── etl_pipeline.py  
│   ├── utils.py  
│   └── __init__.py  
│  
├── requirements.txt  
└── Dockerfile  
```

* `data/`: Directory containing input CSV files.  
* `output/`: Directory where processed data will be stored.  
* `src/`: Directory containing the ETL Spark job script and other helper functions.  
* `README.md`: This documentation file.  
* `Dockerfile`: To build and run the image

# Requirements
Python 3.x  
Apache Spark (preferably version 3.x)  
PySpark  

# Setup
1. Clone the Repository:

    ```bash 
    git clone <repository-url>
    cd <repository-directory>
    ```

2. Install Dependencies:

   Make sure you have Python 3.x installed. Then, install PySpark using pip:

    ```bash
    pip install pyspark
    ```
3. Place Your Data:

    Add your CSV files to the data/ directory. The expected files are:  
    &emsp; accounts.csv  
    &emsp; skus.csv  
    &emsp; invoices.csv  
    &emsp; invoice_line_items.csv  

# Running the ETL Job
I have used Docker for portability. To run the ETL job, use the following command:

```bash 
docker build -t etl-pipeline .
```  
```bash
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output etl-pipeline
```

1. --rm:  
Automatically remove the container when it exits.
2. -v $(pwd)/data:/app/data:  
Mounts your local data directory to the container’s /app/data directory.
3. -v $(pwd)/output:/app/output:  
Mounts your local output directory to the container’s /app/output directory.
4. etl-pipeline:  
The name of the Docker image to run.

# Future State Architecture
Refer image Future State Design.drawio.png