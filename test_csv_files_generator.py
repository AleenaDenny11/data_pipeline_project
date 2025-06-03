import csv
import os
import random
from datetime import datetime, timedelta


DATA_DIR = "data"  
NUM_FILES_TO_GENERATE = 5
RECORDS_PER_FILE = 20 

SENSOR_IDS = ["Kaggle_Sim_A01", "Kaggle_Sim_B02", "Kaggle_Sim_C03", "Weather_Station_Main"]

TEMP_NORMAL_MIN = -5.0   
TEMP_NORMAL_MAX = 35.0   
HUMIDITY_NORMAL_MIN = 0.20 
HUMIDITY_NORMAL_MAX = 0.99 
PRESSURE_NORMAL_MIN = 980.0 
PRESSURE_NORMAL_MAX = 1050.0 



TEMP_VALID_MIN_CONFIG = -50.0 
TEMP_VALID_MAX_CONFIG = 50.0  


def generate_csv_file(filename_prefix, file_index, num_records):
    """Generates a single CSV file with simulated weather sensor data."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
   
    timestamp_str_filename = datetime.now().strftime("%Y%m%d%H%M%S%f")
    filename = f"{filename_prefix}_{timestamp_str_filename}_{file_index}.csv"
    filepath = os.path.join(DATA_DIR, filename)
    
    
    file_has_errors = random.random() < 0.2 
    num_bad_records_in_file = 0
    if file_has_errors:
        num_bad_records_in_file = random.randint(1, min(3, num_records // 2)) 

    print(f"Generating {filepath} (Errors: {file_has_errors}, Bad records: {num_bad_records_in_file})")

    with open(filepath, 'w', newline='') as csvfile:
        
        fieldnames = ['timestamp', 'sensor_id', 'temperature', 'humidity', 'pressure']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

       
        bad_record_indices = random.sample(range(num_records), num_bad_records_in_file)

        current_time = datetime.now() - timedelta(hours=random.randint(1, 24*5)) 

        for i in range(num_records):
        
            error_type = None 
            
            problematic_timestamp_str = None 

            current_time += timedelta(minutes=random.randint(5, 30)) 
            sensor_id_val = random.choice(SENSOR_IDS)
            
            
            temperature_val = round(random.uniform(TEMP_NORMAL_MIN, TEMP_NORMAL_MAX), 2)
            humidity_val = round(random.uniform(HUMIDITY_NORMAL_MIN, HUMIDITY_NORMAL_MAX), 2)
            pressure_val = round(random.uniform(PRESSURE_NORMAL_MIN, PRESSURE_NORMAL_MAX), 2)

            
            if i in bad_record_indices:
                error_type = random.choice(["null_key_sensor_id", "null_key_timestamp", 
                                            "bad_type_temp", "out_of_range_temp_low", 
                                            "out_of_range_temp_high", "null_reading_humidity"])
                print(f"  - Introducing error: '{error_type}' at row {i+1} for sensor '{sensor_id_val}'")
                
                if error_type == "null_key_sensor_id":
                    sensor_id_val = None 
                elif error_type == "null_key_timestamp":
                    problematic_timestamp_str = "NOT_A_VALID_TIMESTAMP"
                elif error_type == "bad_type_temp":
                    temperature_val = "abc" 
                elif error_type == "out_of_range_temp_low":
                    temperature_val = round(TEMP_VALID_MIN_CONFIG - random.uniform(5, 20), 2)
                elif error_type == "out_of_range_temp_high":
                    temperature_val = round(TEMP_VALID_MAX_CONFIG + random.uniform(5, 20), 2)
                elif error_type == "null_reading_humidity":
                    humidity_val = None 
            
            
            
            final_timestamp_value = problematic_timestamp_str if error_type == "null_key_timestamp" else current_time.strftime('%Y-%m-%d %H:%M:%S')

            row_data = {
                'timestamp': final_timestamp_value,
                'sensor_id': sensor_id_val,
                'temperature': temperature_val,
                'humidity': humidity_val,
                'pressure': pressure_val
            }
            
            writer.writerow(row_data)
            
    print(f"Successfully generated: {filepath}")

if __name__ == "__main__":
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created data directory: {DATA_DIR}")

    print(f"\nGenerating {NUM_FILES_TO_GENERATE} sample CSV files in the '{DATA_DIR}' directory...")
    print("Some files/rows may contain deliberate errors for testing validation logic.")
    print(f"Normal temp range for generation: {TEMP_NORMAL_MIN}°C to {TEMP_NORMAL_MAX}°C")
    print(f"Validation temp range (from config): {TEMP_VALID_MIN_CONFIG}°C to {TEMP_VALID_MAX_CONFIG}°C (errors will be outside this)")
    print("-" * 30)

    for i in range(NUM_FILES_TO_GENERATE):
        generate_csv_file(
            filename_prefix="simulated_weather_data", 
            file_index=i+1, 
            num_records=RECORDS_PER_FILE
            )
        

    print("-" * 30)
    print(f"Finished generating {NUM_FILES_TO_GENERATE} files.")
    print(f"You can now run main_pipeline.py to process them from the '{DATA_DIR}' folder.")