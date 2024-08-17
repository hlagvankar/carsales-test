# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install necessary packages including OpenJDK 11 and ca-certificates-java
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    ca-certificates-java \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Apache Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar xvf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2 /usr/local/spark && \
    rm spark-3.1.2-bin-hadoop3.2.tgz

# Set Spark environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Define Python environment variables for PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run etl_job.py when the container launches
CMD ["spark-submit", "src/etl_pipeline.py", "--input_dir", "data", "--output_dir", "output"]
