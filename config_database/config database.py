import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import os

# Change the current working directory
os.chdir("../flights_project")

# Print the current working directory
print("Attuale cartella", os.getcwd())

# Path to the configuration file
config_file_path = os.path.join(os.getcwd(), "config_database", "connessione_database.ini")

# Initialize and read the configuration file
config = configparser.ConfigParser()
config.read(config_file_path)

# Fetch database connection details
db_user = config['database']['user']
db_password = config['database']['password']
db_host = config['database']['host']
db_port = config['database']['port']
db_name = config['database']['name']

# Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Path to the main directory containing CSV files
main_path = "./data_generation/flights_yyyymmdd/"

def create_table_from_csv(csv_path, table_name):
    # Load the CSV into a DataFrame
    df = pd.read_csv(csv_path, low_memory=False)
    
    # Normalizza i nomi delle colonne
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        connection.execute(text(f"""
                CREATE TABLE {table_name} (
                    flight_id UUID,
                    airline TEXT,
                    departure_airport TEXT,
                    arrival_airport TEXT,
                    departure_country TEXT,
                    arrival_country TEXT,
                    city_departure TEXT,
                    city_arrival TEXT,
                    airport_name_departure TEXT,
                    airport_name_arrival TEXT,
                    scheduled_departure TIMESTAMP,
                    scheduled_arrival TIMESTAMP,
                    actual_departure TIMESTAMP,
                    actual_arrival TIMESTAMP,
                    flight_duration BIGINT,
                    status TEXT,
                    date_flight TEXT,
                    day_of_week TEXT,
                    month TEXT,
                    year INTEGER
                )
            """))
    # Use the SQLAlchemy engine with pandas to_sql method
    try:
        with engine.connect() as connection:
            df.to_sql(name=table_name, con=connection, if_exists='fail', index=False, chunksize=1000)
        print(f"Tabella '{table_name}' creata e dati caricati dal file {csv_path}")
    except Exception as e:
        print(f"Errore durante il caricamento del file {csv_path}: {str(e)}")

# Iterate through all subdirectories and CSV files
for root, _, files in os.walk(main_path):
    for file in files:
        if file.endswith('.csv'):
            table_name = os.path.splitext(file)[0]
            csv_path = os.path.join(root, file)
            create_table_from_csv(csv_path, table_name)

print("Tutti i dati sono stati caricati nel database con successo.")