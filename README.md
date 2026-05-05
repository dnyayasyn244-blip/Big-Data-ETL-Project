# Data Mining & Big Data Analysis Project 📊

This project implements an ETL Pipeline using PySpark as requested in the course requirements.

## 🛠️ Project Scope (Based on Section Requirements):
1.  Data Source: Simulation of data retrieval from HDFS.
2.  Processing: Using Spark with Yarn Master configuration.
3.  Transformations: Data cleaned and converted into a Star Schema (Fact and Dimension tables).
4.  Orchestration: (Work in Progress) Prepared for Airflow integration in the next phase.

## 🚀 How to Run:
1. Ensure JAVA_HOME and HADOOP_HOME are correctly set in the script.
2. Install dependencies: pip install pyspark.
3. Run: python src/main_pipeline.py.

## 📂 Architecture:
The data is transformed from a flat structure into:
- Fact Table: Contains quantitative data (prices, dates).
- Dimension Table: Contains descriptive attributes (product categories).
-# Big-Data-ETL-Project
 A PySpark ETL pipeline project for data processing and Star Schema transformation.
