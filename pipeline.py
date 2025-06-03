import os
import time
import shutil
import pandas as pd
import numpy as np 
import mysql.connector
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
import configparser
from datetime import datetime

config = configparser.ConfigParser()
if not os.path.exists('config.ini'):
    print("CRITICAL: config.ini not found. Pipeline cannot start.")
    exit(1) 
config.read('config.ini')

try:
    DATA_FOLDER = config.get('DEFAULT', 'DATA_FOLDER', fallback='data')
    QUARANTINE_FOLDER = config.get('DEFAULT', 'QUARANTINE_FOLDER', fallback='quarantine')
    PROCESSED_FOLDER = config.get('DEFAULT', 'PROCESSED_FOLDER', fallback='processed')
    LOG_FILE = config.get('DEFAULT', 'LOG_FILE', fallback='logs/pipeline.log')
    MONITOR_INTERVAL = config.getint('DEFAULT', 'MONITOR_INTERVAL_SECONDS', fallback=5)

    DB_HOST = config.get('DATABASE', 'HOST')
    DB_USER = config.get('DATABASE', 'USER')
    DB_PASSWORD = config.get('DATABASE', 'PASSWORD')
    DB_NAME = config.get('DATABASE', 'DATABASE')

    TEMP_MIN = config.getfloat('VALIDATION', 'TEMP_MIN', fallback=-50.0)
    TEMP_MAX = config.getfloat('VALIDATION', 'TEMP_MAX', fallback=50.0)
    
    HUMIDITY_MIN = config.getfloat('VALIDATION', 'HUMIDITY_MIN', fallback=0.0) 
    HUMIDITY_MAX = config.getfloat('VALIDATION', 'HUMIDITY_MAX', fallback=1.0) 
    PRESSURE_MIN = config.getfloat('VALIDATION', 'PRESSURE_MIN', fallback=900.0)
    PRESSURE_MAX = config.getfloat('VALIDATION', 'PRESSURE_MAX', fallback=1100.0) 

except (configparser.NoSectionError, configparser.NoOptionError, KeyError) as e:
    print(f"CRITICAL: Missing or invalid configuration in config.ini: {e}. Pipeline cannot start.")
    exit(1)


os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler() 
    ]
)
logger = logging.getLogger("MainPipelineStrict")


os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(QUARANTINE_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

def get_db_connection(retry_count=3, delay=5):
    attempt = 0
    while attempt < retry_count:
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                connect_timeout=10 
            )
            if conn.is_connected():
                logger.info("Successfully connected to database.")
                return conn
        except mysql.connector.Error as e:
            logger.error(f"Database connection failed (attempt {attempt + 1}/{retry_count}): {e}")
            attempt += 1
            if attempt < retry_count:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("Max retry attempts reached for DB connection.")
                return None
    return None

def validate_and_transform_data_strict(df_original, file_name):
    """
    STRICT VALIDATION: Validates data. If ANY row fails critical validation,
    the entire file is marked for quarantine.
    Returns a tuple: (valid_df_if_all_ok_or_None, file_level_errors_list)
    """
    df = df_original.copy() # Work on a copy
    file_level_errors = []
    original_row_count = len(df)
    logger.info(f"File '{file_name}': Starting STRICT validation for {original_row_count} rows.")

   
    expected_columns = ['timestamp', 'sensor_id', 'temperature', 'humidity', 'pressure']
    key_readings_cols = ['temperature', 'humidity', 'pressure'] 

    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        msg = f"File '{file_name}': Missing critical columns: {', '.join(missing_cols)}. Quarantining."
        logger.error(msg)
        file_level_errors.append(msg)
        return None, file_level_errors

   
    df['sensor_id'] = df['sensor_id'].replace('', pd.NA)
    df['timestamp'] = df['timestamp'].replace('', pd.NA)
    for col in key_readings_cols:
        df[col] = df[col].replace('', pd.NA) 
  
    for index, row in df.iterrows():
        row_errors = []
       
        if pd.isnull(row['sensor_id']):
            row_errors.append(f"Row {index+2}: 'sensor_id' is null.") 

       
        if pd.isnull(row['timestamp']):
            row_errors.append(f"Row {index+2}: 'timestamp' string is null/empty.")
        else:
            try:
                pd.to_datetime(row['timestamp'])
            except (ValueError, TypeError, OverflowError):
                row_errors.append(f"Row {index+2}: 'timestamp' ('{row['timestamp']}') is unparsable.")
        
        
        if pd.isnull(row['temperature']):
            row_errors.append(f"Row {index+2}: 'temperature' is null.")
        else:
            try:
                temp_val = float(row['temperature'])
                if not (TEMP_MIN <= temp_val <= TEMP_MAX):
                    row_errors.append(f"Row {index+2}: 'temperature' ({temp_val}) out of range [{TEMP_MIN}, {TEMP_MAX}].")
            except (ValueError, TypeError):
                row_errors.append(f"Row {index+2}: 'temperature' ('{row['temperature']}') is not a valid number.")

        
        if pd.isnull(row['humidity']):
            row_errors.append(f"Row {index+2}: 'humidity' is null.")
        else:
            try:
                hum_val = float(row['humidity'])
                if not (HUMIDITY_MIN <= hum_val <= HUMIDITY_MAX):
                     row_errors.append(f"Row {index+2}: 'humidity' ({hum_val}) out of range [{HUMIDITY_MIN}, {HUMIDITY_MAX}].")
            except (ValueError, TypeError):
                row_errors.append(f"Row {index+2}: 'humidity' ('{row['humidity']}') is not a valid number.")
        
        
        if pd.isnull(row['pressure']):
            row_errors.append(f"Row {index+2}: 'pressure' is null.")
        else:
            try:
                pres_val = float(row['pressure'])
                if not (PRESSURE_MIN <= pres_val <= PRESSURE_MAX):
                     row_errors.append(f"Row {index+2}: 'pressure' ({pres_val}) out of range [{PRESSURE_MIN}, {PRESSURE_MAX}].")
            except (ValueError, TypeError):
                row_errors.append(f"Row {index+2}: 'pressure' ('{row['pressure']}') is not a valid number.")

        if row_errors:
            full_error_msg = f"File '{file_name}': Row {index+2} (original index {index}) failed validation. Errors: {'; '.join(row_errors)}. Data: {row.to_dict()}. Quarantining file."
            logger.error(full_error_msg)
            file_level_errors.append(f"Validation failed at row {index+2}: {'; '.join(row_errors)}")
            return None, file_level_errors 

    
    logger.info(f"File '{file_name}': All {original_row_count} rows passed strict validation.")

    try:
        df['timestamp'] = pd.to_datetime(df['timestamp']) 
        for col in key_readings_cols:
            df[col] = pd.to_numeric(df[col]) 
    except Exception as e:
        
        msg = f"File '{file_name}': Error during bulk transformation after strict validation: {e}. Quarantining."
        logger.error(msg)
        file_level_errors.append(msg)
        return None, file_level_errors

    
    logger.info(f"File '{file_name}': Strict validation and transformation complete. All {len(df)} rows are valid.")
    return df, file_level_errors 


def calculate_aggregates(df, file_name_original):
    if df.empty:
        return pd.DataFrame()
    aggregations = []
    df['timestamp'] = pd.to_datetime(df['timestamp']) 
    for sensor_id, group in df.groupby('sensor_id'):
        agg_time = group['timestamp'].min() 
        for metric_col in ['temperature', 'humidity', 'pressure']:
            if metric_col in group.columns:
                series = group[metric_col].dropna() 
                if not series.empty:
                    aggregations.append({
                        'sensor_id': sensor_id,
                        'file_name': file_name_original, 
                        'aggregation_time': agg_time, 
                        'metric_name': metric_col,
                        'min_value': series.min(),
                        'max_value': series.max(),
                        'avg_value': series.mean(),
                        'std_dev_value': series.std() if len(series) > 1 else 0.0,
                        'record_count': len(series)
                    })
    return pd.DataFrame(aggregations)


def store_data(conn, raw_df, agg_df, file_name_original):
    cursor = None
    try:
        cursor = conn.cursor()
        if not raw_df.empty:
            raw_data_to_insert = []
            for _, row in raw_df.iterrows():
                ts = row['timestamp'].to_pydatetime() if isinstance(row['timestamp'], pd.Timestamp) else row['timestamp']
                raw_data_to_insert.append((
                    row['sensor_id'], ts, row.get('temperature'), 
                    row.get('humidity'), row.get('pressure'), file_name_original ))
            if raw_data_to_insert:
                sql_raw = "INSERT INTO raw_sensor_data (sensor_id, timestamp, temperature, humidity, pressure, file_name) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.executemany(sql_raw, raw_data_to_insert)
                logger.info(f"File '{file_name_original}': Inserted {cursor.rowcount} rows into 'raw_sensor_data'.")

        if not agg_df.empty:
            agg_data_to_insert = []
            for _, row in agg_df.iterrows():
                agg_ts = row['aggregation_time'].to_pydatetime() if isinstance(row['aggregation_time'], pd.Timestamp) else row['aggregation_time']
                agg_data_to_insert.append((
                    row['sensor_id'], row['file_name'], agg_ts, row['metric_name'],
                    row.get('min_value'), row.get('max_value'), row.get('avg_value'),
                    row.get('std_dev_value'), row.get('record_count')))
            if agg_data_to_insert:
                sql_agg = """
                INSERT INTO aggregated_sensor_data
                (sensor_id, file_name, aggregation_time, metric_name, min_value, max_value, avg_value, std_dev_value, record_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    min_value=VALUES(min_value), max_value=VALUES(max_value),
                    avg_value=VALUES(avg_value), std_dev_value=VALUES(std_dev_value),
                    record_count=VALUES(record_count), processed_at=NOW()"""
                cursor.executemany(sql_agg, agg_data_to_insert)
                logger.info(f"File '{file_name_original}': Inserted/Updated {cursor.rowcount} rows in 'aggregated_sensor_data'.")
        conn.commit()
        return True
    except mysql.connector.Error as e:
        logger.error(f"Database error storing data for '{file_name_original}': {e}")
        if conn and conn.is_connected():
            try: conn.rollback()
            except Exception as rb_err: logger.error(f"Error during rollback: {rb_err}")
        return False
    except Exception as e: 
        logger.error(f"Unexpected error storing data for '{file_name_original}': {e}", exc_info=True)
        if conn and conn.is_connected():
            try: conn.rollback()
            except Exception as rb_err: logger.error(f"Error during rollback: {rb_err}")
        return False
    finally:
        if cursor: cursor.close()


def log_quarantine_reason(file_name, reason):
    quarantine_log_file = os.path.join(QUARANTINE_FOLDER, "quarantine_log.txt")
    try:
        with open(quarantine_log_file, "a") as f:
            f.write(f"{datetime.now().isoformat()} - File: {file_name}, Reason: {reason}\n")
    except IOError as e:
        logger.error(f"Failed to write to quarantine log for {file_name}: {e}")
    logger.warning(f"File '{file_name}' moved to quarantine. Reason: {reason}")

def process_file(filepath):
    file_name_original = os.path.basename(filepath)
    logger.info(f"Processing new file: {filepath}")
    if not os.path.exists(filepath):
        logger.warning(f"File '{filepath}' no longer exists. Skipping.")
        return
    temp_filepath = None
    try:
        temp_filename = f"processing_{file_name_original}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        temp_filepath = os.path.join(os.path.dirname(filepath), temp_filename)
        shutil.copy2(filepath, temp_filepath)
        logger.info(f"Copied '{file_name_original}' to '{temp_filename}' for processing.")
        try:
            df = pd.read_csv(temp_filepath, na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'], keep_default_na=True, dtype=str) # Read all as string initially for precise row-by-row validation
            if df.empty:
                logger.warning(f"File '{file_name_original}' (from {temp_filename}) is empty. Quarantining original.")
                shutil.move(filepath, os.path.join(QUARANTINE_FOLDER, file_name_original))
                log_quarantine_reason(file_name_original, "File is empty or contains only headers after NA filtering.")
                return
        except pd.errors.EmptyDataError:
            
            shutil.move(filepath, os.path.join(QUARANTINE_FOLDER, file_name_original))
            log_quarantine_reason(file_name_original, "File is empty (pandas EmptyDataError).")
            return
        except Exception as e: 
            
            shutil.move(filepath, os.path.join(QUARANTINE_FOLDER, file_name_original))
            log_quarantine_reason(file_name_original, f"CSV parsing error: {e}")
            return
        finally:
            if temp_filepath and os.path.exists(temp_filepath): 
                os.remove(temp_filepath)
                logger.info(f"Removed temporary processing file: {temp_filepath}")

       
        valid_df, file_level_errors = validate_and_transform_data_strict(df, file_name_original)

        if valid_df is None or valid_df.empty: 
            error_summary = "; ".join(file_level_errors) if file_level_errors else "Validation failed and no data remained (strict)."
            logger.warning(f"Strict validation failed for '{file_name_original}'. Quarantining. Summary: {error_summary}")
            shutil.move(filepath, os.path.join(QUARANTINE_FOLDER, file_name_original))
            log_quarantine_reason(file_name_original, error_summary)
            return
        
        agg_df = calculate_aggregates(valid_df, file_name_original)
        db_conn = None
        try:
            db_conn = get_db_connection()
            if db_conn:
                if store_data(db_conn, valid_df, agg_df, file_name_original):
                    logger.info(f"Successfully stored data from '{file_name_original}'. Moving to processed.")
                    shutil.move(filepath, os.path.join(PROCESSED_FOLDER, file_name_original))
                else:
                    logger.error(f"Failed to store data for '{file_name_original}' in DB. File remains in data folder for retry.")
            else:
                logger.error(f"Could not connect to database. File '{file_name_original}' not processed, remains in data folder.")
        finally:
            if db_conn and db_conn.is_connected():
                db_conn.close()
                logger.info(f"Database connection closed for file '{file_name_original}'.")
    except FileNotFoundError:
        logger.warning(f"File '{filepath}' (original or temp) not found during processing.")
    except IOError as e: 
        logger.error(f"IOError processing file '{file_name_original}': {e}. Attempting to quarantine original.")
        if os.path.exists(filepath):
            try:
                shutil.move(filepath, os.path.join(QUARANTINE_FOLDER, file_name_original))
                log_quarantine_reason(file_name_original, f"IOError during processing: {e}")
            except Exception as move_err: logger.error(f"Could not move '{file_name_original}' to quarantine after IOError: {move_err}")
        else: logger.warning(f"Original file '{file_name_original}' no longer exists to quarantine after IOError.")
    except Exception as e: 
        logger.error(f"Unhandled error processing file '{file_name_original}': {e}", exc_info=True)
        if os.path.exists(filepath):
            try:
                shutil.move(filepath, os.path.join(QUARANTINE_FOLDER, file_name_original))
                log_quarantine_reason(file_name_original, f"Unhandled processing error: {e}")
            except Exception as move_err: logger.error(f"Could not move '{file_name_original}' to quarantine after unhandled error: {move_err}")
        else: logger.warning(f"Original file '{file_name_original}' no longer exists to quarantine after unhandled error.")
    finally: 
        if temp_filepath and os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
                logger.info(f"Ensured removal of temporary processing file in finally block: {temp_filepath}")
            except OSError as e: logger.error(f"Error removing temp file {temp_filepath} in finally block: {e}")



class CSVFileHandler(FileSystemEventHandler):
    def __init__(self):
        self.recently_processed = set() 
        self.processing_lock_timeout = 30 
    def _should_process(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.endswith('.csv'): return False
        if filename.startswith('processing_'): return False
        if filepath in self.recently_processed:
            logger.debug(f"File '{filepath}' is in recently_processed set. Skipping event.")
            return False
        return True
    def _add_to_processed(self, filepath): self.recently_processed.add(filepath)
    def on_created(self, event):
        if not event.is_directory:
            filepath = event.src_path
            if self._should_process(filepath):
                logger.info(f"Watchdog: File created: {filepath}")
                self._add_to_processed(filepath)
                time.sleep(1) 
                if os.path.exists(filepath): process_file(filepath)
                else:
                    logger.warning(f"Watchdog: File {filepath} disappeared before processing on create.")
                    if filepath in self.recently_processed: self.recently_processed.remove(filepath)
    def on_moved(self, event): 
        if not event.is_directory:
            filepath = event.dest_path 
            if os.path.commonpath([os.path.abspath(DATA_FOLDER), os.path.abspath(filepath)]) == os.path.abspath(DATA_FOLDER):
                if self._should_process(filepath):
                    logger.info(f"Watchdog: File moved into monitored folder: {filepath}")
                    self._add_to_processed(filepath)
                    time.sleep(1) 
                    if os.path.exists(filepath): process_file(filepath)
                    else:
                        logger.warning(f"Watchdog: File {filepath} disappeared before processing on move.")
                        if filepath in self.recently_processed: self.recently_processed.remove(filepath)

if __name__ == "__main__":
    logger.info("==================================================")
    logger.info("Starting Real-Time Data Pipeline (STRICT VALIDATION)...") 
    logger.info(f"Monitoring folder: {os.path.abspath(DATA_FOLDER)}")
    
    event_handler = CSVFileHandler()
    observer = Observer()
    try:
        observer.schedule(event_handler, DATA_FOLDER, recursive=False)
        observer.start()
    except Exception as e:
        logger.critical(f"Failed to start watchdog observer: {e}", exc_info=True)
        exit(1)

    try:
        logger.info("Performing initial scan of data folder...")
        for filename in os.listdir(DATA_FOLDER):
            filepath = os.path.join(DATA_FOLDER, filename)
            if os.path.isfile(filepath) and event_handler._should_process(filepath):
                logger.info(f"Initial scan: Found file {filepath}. Processing.")
                event_handler._add_to_processed(filepath) 
                process_file(filepath)
        logger.info(f"Initial scan complete. Now monitoring...")
        while True:
            
            for filename in os.listdir(DATA_FOLDER):
                filepath = os.path.join(DATA_FOLDER, filename)
                if os.path.isfile(filepath) and event_handler._should_process(filepath):
                    logger.info(f"Periodic scan: Found unprocessed file {filepath}. Processing.")
                    event_handler._add_to_processed(filepath)
                    process_file(filepath)
            if len(event_handler.recently_processed) > 100: 
                logger.warning("Clearing recently_processed set (fallback).")
                event_handler.recently_processed.clear()
            time.sleep(MONITOR_INTERVAL)
    except KeyboardInterrupt: logger.info("Pipeline shutdown requested (KeyboardInterrupt).")
    except Exception as e: logger.critical(f"Critical error in main loop: {e}", exc_info=True)
    finally:
        logger.info("Stopping watchdog observer...")
        observer.stop()
        observer.join() 
        logger.info("Pipeline shut down gracefully.")