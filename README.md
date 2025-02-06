# Clickstream Analysis & Anomaly Detection with Kafka & Spark Structured Streaming
This project is a real-time Clickstream Data Processing Pipeline built using Apache Kafka, Apache Spark Streaming, PostgreSQL, and Google BigQuery. It ingests, processes, analyzes, and detects anomalies in web clickstream data to derive actionable insights.

## Features
1. Kafka for event Streaming :Captures raw clickstream data in real time.

2. Spark Streaming for Processing: Transforms and enriches data before storage.

3. PostgreSQL for Detailed Logs: Stores raw clickstream events for further analysis.

4. BigQuery for Aggregated Insights: Stores processed data for analytical queries and dashboards.

5. Anomaly Detection: Identifies unusual user behavior and posts alerts back to a Kafka topic.

## Technologies Used
#### Apache Kafka
#### Apache Spark Structured Streaming
#### PostgresSQL
#### Python

## Architecture
```mermaid
graph LR;
    A[Clickstream Data] --> B[Kafka Producer];
    B --> C[Kafka Topic];
    C --> D[Spark Streaming];
    D --> E[PostgreSQL / BigQuery];
    D --> F[Anomaly Detection];
    F --> G[Kafka Anomaly Topic];
```
## Setup and Deployment
###  Deploy Kafka on GCP VM:
#### 1. Install Java (Required for Kafka & Spark)
```bash
sudo apt update
sudo apt install -y openjdk-11-jdk
java -version
````
#### 2. Install Apache Kafka
##### Download and Extract Kafka
```bash
wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
mv kafka_2.13-3.6.1 kafka
cd kafka
```
##### Start Zookeeper
```bash
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
```
Verify Zookeeper
```bash
telnet localhost 2181
```

##### Start Kafka broker
```bash
bin/kafka-server-start.sh -daemon config/server.properties
```
Verify Kafka brokers
```bash
telnet localhost 9092
```
### Install Apache Spark
#### Download and Extract Spark
```bash
wget https://downloads.apache.org/spark/3.5.4/spark-3.5.4-bin-hadoop3.tgz
tar -xzf spark-3.5.4-bin-hadoop3.tgz
mv spark-3.5.4-bin-hadoop3 spark
```
#### Set Environment Variables
```bash
echo 'export SPARK_HOME=~/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=/usr/bin/python3' >> ~/.bashrc
source ~/.bashrc
```
### Install Jupyter Lab
```bash
pip install jupyterlab findspark
```
#### Configure Jupyter to Use PySpark
```bash
echo 'export PYSPARK_DRIVER_PYTHON=jupyter' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON_OPTS="lab --ip=0.0.0.0 --port=8888 --no-browser"' >> ~/.bashrc
source ~/.bashrc
```
#### Run Jupyter Lab
```bash
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
```

















    
