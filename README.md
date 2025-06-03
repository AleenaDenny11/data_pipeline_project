# Real-Time Weather Sensor Data Pipeline

## Overview

This project implements a real-time data pipeline for ingesting, validating, transforming, and storing weather sensor data. The system monitors a directory for incoming CSV files, processes each file, and stores both the raw and aggregated data into a MySQL database. Invalid data is quarantined for inspection.

The dataset used is from Kaggle:
[Weather Dataset by Muthu](https://www.kaggle.com/datasets/muthuj7/weather-dataset)

## Features

- Monitors a folder named `data/` for new CSV files every 5–10 seconds
- Validates incoming data for:
  - Missing key fields (sensor ID, timestamp, reading)
  - Correct data types
  - Acceptable sensor value ranges
- Transforms valid data into a standardized format
- Computes aggregated metrics: minimum, maximum, average, and standard deviation per sensor type
- Stores raw and aggregated data in a MySQL database
- Moves invalid data to `quarantine/` with detailed error logs
- Logs processing steps and errors for audit and debugging


