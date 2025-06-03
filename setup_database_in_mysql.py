import mysql.connector
from mysql.connector import errorcode
import configparser

def create_database(cursor, db_name):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET 'utf8'")
        print(f"Database {db_name} checked/created successfully.")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DB_CREATE_EXISTS:
            print(f"Database {db_name} already exists.")
        else:
            print(f"Failed creating database: {err}")
            exit(1)

def main():
    config = configparser.ConfigParser()
    
    if not os.path.exists('config.ini'):
        print("Error: config.ini not found. Please ensure it's in the current directory.")
        return
    config.read('config.ini')

    try:
        db_host = config['DATABASE']['HOST']
        db_user = config['DATABASE']['USER']
        db_password = config['DATABASE']['PASSWORD']
        
        db_name_target = config['DATABASE']['DATABASE']
    except KeyError as e:
        print(f"Error: Missing configuration key in config.ini: {e}")
        return

    try:
        
        cnx = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password
        )
        cursor = cnx.cursor()
        print("Successfully connected to MySQL server.")

        create_database(cursor, db_name_target)
        cursor.execute(f"USE {db_name_target}") 
        print(f"Using database: {db_name_target}")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your MySQL user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database {db_name_target} does not exist and could not be selected (should have been created).")
        else:
            print(f"MySQL Connection Error: {err}")
        return 

    
    raw_data_table = """
    CREATE TABLE IF NOT EXISTS raw_sensor_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sensor_id VARCHAR(255) NOT NULL,
        timestamp DATETIME NOT NULL,
        temperature FLOAT,         -- Temperature (C) from Kaggle dataset
        humidity FLOAT,            -- Humidity from Kaggle dataset
        pressure FLOAT,            -- Pressure (millibars) from Kaggle dataset
        file_name VARCHAR(255),    -- Name of the CSV chunk file
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_sensor_id (sensor_id),
        INDEX idx_timestamp (timestamp),
        INDEX idx_file_name (file_name) -- Optional: if you query by file often
    ) ENGINE=InnoDB;
    """
    try:
        cursor.execute(raw_data_table)
        print("Table 'raw_sensor_data' checked/created successfully.")
    except mysql.connector.Error as err:
        print(f"Failed creating 'raw_sensor_data' table: {err}")


    
    aggregated_data_table = """
    CREATE TABLE IF NOT EXISTS aggregated_sensor_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sensor_id VARCHAR(255) NOT NULL,
        file_name VARCHAR(255) NOT NULL,      -- Source file for this aggregation batch
        aggregation_time DATETIME NOT NULL,   -- Typically the min timestamp from the file for this sensor
        metric_name VARCHAR(50) NOT NULL,     -- e.g., 'temperature', 'humidity', 'pressure'
        min_value FLOAT,
        max_value FLOAT,
        avg_value FLOAT,
        std_dev_value FLOAT,
        record_count INT,                     -- Number of records used for this aggregation
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_sensor_file_metric_time (sensor_id, file_name, metric_name, aggregation_time),
        INDEX idx_agg_sensor_file (sensor_id, file_name),
        INDEX idx_agg_metric_time (metric_name, aggregation_time)
    ) ENGINE=InnoDB;
    """
    try:
        cursor.execute(aggregated_data_table)
        print("Table 'aggregated_sensor_data' checked/created successfully.")
    except mysql.connector.Error as err:
        print(f"Failed creating 'aggregated_sensor_data' table: {err}")


    


    if cnx.is_connected():
        cursor.close()
        cnx.close()
        print("MySQL connection closed.")

if __name__ == "__main__":
    
    import os 
    main()