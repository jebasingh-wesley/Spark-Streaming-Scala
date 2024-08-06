# Streaming Data Processing with Scala

## Overview

This repository contains Scala scripts for various streaming data processing tasks using Apache Kafka and Spark. The scripts demonstrate how to handle data streams from different sources, including Kafka and HTTP APIs, and process them using Spark. 

### Repository Files

1. **FileStream1.scala**
   - **Description**: This script demonstrates how to process data streams from a file source using Spark Streaming. It includes examples of reading data from files and performing basic transformations and actions.

2. **KafkaStream3.scala**
   - **Description**: This script illustrates how to work with streaming data from Apache Kafka. It shows how to consume messages from a Kafka topic and process them using Spark Streaming.

3. **SocketStream2.scala**
   - **Description**: This script showcases how to handle data streams from a socket source. It includes examples of connecting to a network socket, receiving data streams, and performing real-time processing.

4. **Usecase1KafkaSql.scala**
   - **Description**: This script demonstrates a specific use case involving Kafka and Spark SQL. It shows how to use Spark SQL to query and analyze streaming data from a Kafka source.

5. **Usecase2HttpAPIKafkaSparkESKibana.scala**
   - **Description**: This script covers a more complex use case involving multiple technologies. It integrates HTTP APIs, Kafka, Spark, Elasticsearch (ES), and Kibana. The script demonstrates how to ingest data from an HTTP API, process it with Spark, store it in Elasticsearch, and visualize it with Kibana.

### Getting Started

To get started with the scripts in this repository:

1. **Set Up Kafka and Spark**
   - Ensure that Apache Kafka and Apache Spark are properly installed and configured.

2. **Run FileStream1.scala**
   - Compile and execute this script to see how to process data from a file source.

3. **Run KafkaStream3.scala**
   - Compile and execute this script to consume and process streaming data from Kafka.

4. **Run SocketStream2.scala**
   - Compile and execute this script to handle data streams from a socket source.

5. **Run Usecase1KafkaSql.scala**
   - Compile and execute this script to analyze Kafka streaming data using Spark SQL.

6. **Run Usecase2HttpAPIKafkaSparkESKibana.scala**
   - Compile and execute this script for a comprehensive use case involving HTTP APIs, Kafka, Spark, Elasticsearch, and Kibana.

### Repository Structure

- **FileStream1.scala**
  - Example of file-based streaming data processing.

- **KafkaStream3.scala**
  - Example of Kafka-based streaming data processing.

- **SocketStream2.scala**
  - Example of socket-based streaming data processing.

- **Usecase1KafkaSql.scala**
  - Kafka and Spark SQL integration use case.

- **Usecase2HttpAPIKafkaSparkESKibana.scala**
  - Complex use case involving HTTP APIs, Kafka, Spark, Elasticsearch, and Kibana.

Feel free to explore the scripts and adapt them to your streaming data processing needs. If you encounter any issues or have questions, please open an issue in this repository.

Happy streaming!

---

This README provides a clear description of each script and guides users on how to get started with the examples provided in the repository.
