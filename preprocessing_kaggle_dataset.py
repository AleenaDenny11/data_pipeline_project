import pandas as pd
import os
from datetime import datetime


KAGGLE_CSV_PATH = r"D:\jupiterProjectsAleena\real_time_pipeline\archive\weatherHistory.csv" #path to dataset
OUTPUT_DATA_FOLDER = 'data' 
ROWS_PER_CHUNK = 5000  
SENSOR_IDS = ["Kaggle_Weather_01", "Kaggle_Weather_02", "Kaggle_Weather_03"] 

def preprocess_and_chunk_data():
    if not os.path.exists(KAGGLE_CSV_PATH):
        print(f"Error: no CSV file found at '{KAGGLE_CSV_PATH}'. Please update the path.")
        return

    print(f"Loading Kaggle data from: {KAGGLE_CSV_PATH}")
    try:
        df = pd.read_csv(KAGGLE_CSV_PATH)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    print("Preprocessing data...")
    
    df.rename(columns={
        'Formatted Date': 'timestamp',
        'Temperature (C)': 'temperature',
        'Humidity': 'humidity', 
        'Pressure (millibars)': 'pressure'
    }, inplace=True)
    
    required_cols = ['timestamp', 'temperature', 'humidity', 'pressure']
    df = df[required_cols].copy() 

    
    df['sensor_id'] = [SENSOR_IDS[i % len(SENSOR_IDS)] for i in range(len(df))]
    
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True) 
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error parsing or formatting timestamp: {e}")
        
        df.dropna(subset=['timestamp'], inplace=True)
        
    df.dropna(subset=['temperature', 'humidity', 'pressure', 'sensor_id', 'timestamp'], how='any', inplace=True)

    if df.empty:
        print("No data left after preprocessing. Check column names and data integrity.")
        return

    print(f"Total valid rows after preprocessing: {len(df)}")
    os.makedirs(OUTPUT_DATA_FOLDER, exist_ok=True)
    
    
    num_chunks = (len(df) - 1) // ROWS_PER_CHUNK + 1
    print(f"Splitting into {num_chunks} chunks of (up to) {ROWS_PER_CHUNK} rows each...")

    for i in range(num_chunks):
        start_row = i * ROWS_PER_CHUNK
        end_row = start_row + ROWS_PER_CHUNK
        chunk_df = df.iloc[start_row:end_row]

        if not chunk_df.empty:
            
            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S%f")
            chunk_filename = f"kaggle_weather_chunk_{timestamp_str}_{i+1}.csv"
            chunk_filepath = os.path.join(OUTPUT_DATA_FOLDER, chunk_filename)
            
            
            output_columns = ['timestamp', 'sensor_id', 'temperature', 'humidity', 'pressure']
            chunk_df_final = chunk_df[output_columns]

            chunk_df_final.to_csv(chunk_filepath, index=False)
            print(f"Saved chunk {i+1}: {chunk_filepath}")
        else:
            print(f"Skipping empty chunk {i+1}")

    print(f"\nFinished. {num_chunks} CSV files have been created in the '{OUTPUT_DATA_FOLDER}' directory.")
    print(f"You can now run your main_pipeline.py. It will process these files.")
    print(f"If the pipeline is already running, it should start picking them up.")

if __name__ == "__main__":
    
    preprocess_and_chunk_data()