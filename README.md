# 🛠️ ETL Data Pipeline 

A simple ETL (Extract, Transform, Load) data pipeline built with Python. This project demonstrates core data engineering concepts by processing data from a CSV file, transforming it using `pandas`, and loading it into a SQLite database.

---

## 🚀 Overview

This project was created as part of a Data Engineering self testing project to demonstrate:

- 📥 **Extraction** of structured data from a CSV file
- 🔄 **Transformation** of raw data (e.g., cleaning, computed columns)
- 🗃️ **Loading** the transformed data into a SQL database (SQLite)

It serves as a minimal working example of building a modular and testable ETL pipeline.

---

## 📂 Project Structure

ETL-DATA-PIPELINE/
├── data/
│ └── (csv files once downloaded) 
│
├── (etl_database.db once saved)
│
├── etl/
│ └── main.py 
│
├── tests/
│ └── test_etl.py
│
├── README.md
└── .gitignore

## Setup

Before running the ETL pipeline, make sure to download the necessary CSV files.  
These files are not included in the repository due to size limitations and are required for the ETL process to work correctly.

Place the CSV files in the `data/` directory as expected by the pipeline:


Once the files are in place, you can run the ETL process as usual:


python etl/main.py



### Download CSV Files

You can download the CSV files from:

curl  https://storage.googleapis.com/bdt-beam/users_v.csv -o data/users.csv

curl  https://storage.googleapis.com/bdt-beam/orders_v_2022.csv -o data/orders.csv
