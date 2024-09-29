# Sistemare script perché deve prendere i dati dalla tabella finale

import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import os

# Cambia la directory di lavoro
os.chdir("../flights_project")
print("Attuale cartella:", os.getcwd())

# Percorso del file di configurazione
config_file_path = os.path.join(os.getcwd(), "populate_staging", "connessione_database.ini")

# Inizializza e leggi il file di configurazione
config = configparser.ConfigParser()
config.read(config_file_path)

# Recupera i dettagli di connessione al database
db_user = config['database']['user']
db_password = config['database']['password']
db_host = config['database']['host']
db_port = config['database']['port']
db_name = config['database']['name']

# Crea il motore SQLAlchemy
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

table_name = "stagingflights"

drop_table_sql = f"""
DROP TABLE IF EXISTS {table_name}
"""

# Crea la tabella StagingFlights se non esiste già
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
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
"""
with engine.connect() as connection:
    connection.execute(text(drop_table_sql))
    connection.execute(text(create_table_sql))

# Percorso della cartella contenente i file CSV
csv_folder_path = './data_operations/flights_yyyymmdd'

# Itera attraverso tutti i file nella cartella
for root, _, files in os.walk(csv_folder_path):
    for file in files:
        if file.endswith('.csv'):
            csv_path = os.path.join(root, file)
            
            print(f"Caricamento del file: {csv_path}")

            # Carica il CSV in un DataFrame
            df = pd.read_csv(csv_path)

            # Inserisci i dati nella tabella StagingFlights
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

            print(f"File {file} caricato con successo!")

print("Tutti i file CSV sono stati caricati nella tabella 'StagingFlights' con successo!")
