# PySpark Kafka Data Pipeline

This project demonstrates a simplified data pipeline using **PySpark** and **Apache Kafka** for generating, processing, and consuming real-time data.

## 📋 Overview

The project simulates a complete data pipeline that:
- Dynamically generates data using a Kafka producer
- Processes data in real-time using PySpark
- Consumes and displays results in the console

## 🏗️ Architecture
+----------------+ +-------------+ +-----------------+ +---------------+
| Data Generator | --> | Kafka Topic | --> | Spark Streaming | --> | Console Output|
| (Producer) | | (test-topic)| | (Consumer) | | |
+----------------+ +-------------+ +-----------------+ +---------------+


## 📁 Project Structure
📁 pyspark-kafka-pipeline/
├── 🐳 docker-compose.yml # Container orchestration
├── 📝 README.md # Project documentation
├── 🐍 main.py # Main PySpark application
├── 📋 requirements.txt # Python dependencies
└── 🐋 Dockerfile.spark # Spark Docker image


## 🚀 Quick Start

### Prerequisites
- Docker
- Docker Compose

### Execution Steps

1. **Start infrastructure:**
```bash
docker-compose up -d

# Verify running services:
docker-compose ps

# Access Spark container
docker exec -it spark-master bash

# Run the application
cd /opt/app
python main.py

 🔧  Services
Kafka & Zookeeper
Zookeeper: Port 2181

Kafka Broker: Port 9092 (internal) and 29092 (external)

Spark
Spark Master: Port 7077

Web UI: Port 8080

# 📊 Data Flow
Data Generation: Continuous JSON message generation

Kafka Production: Data sent to test-topic

Spark Consumption: PySpark processes streaming data

Console Output: Real-time results display

🛠️ Technology Stack

Apache Spark 3.5.0 - Distributed processing
Apache Kafka - Distributed messaging
Docker - Containerization
Python 3 - Programming language


📝 Data Schema
{
  "id": 123,
  "text": "Test message 45",
  "timestamp": 1691234567.89
}

🔮 Potential Extensions
Store data in Parquet/CSV formats
Database integration
Complex data transformations
Kubernetes cluster deployment
Monitoring with Prometheus/Grafana

🐛 Troubleshooting
Verify Kafka is running

docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

Check service logs
docker-compose logs kafka
docker-compose logs spark

Note: This is a simulation project to demonstrate data engineering concepts with PySpark and Kafka.