if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    import pandas as pd
    from sqlalchemy import create_engine, text
    import configparser
    import os

    # # Change the current working directory to the dataset directory
    # os.chdir("../src")

    # Print the current working directory
    # print("Current working directory:", os.getcwd())

    # Path to the configuration file
    config_file_path = os.path.join(os.getcwd(), "config_database", "connessione_database.ini")
    print(config_file_path)

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
    main_path = "./flights_yyyymmdd/"

    def create_table_from_csv(csv_path, table_name):
        # Load the CSV into a DataFrame
        df = pd.read_csv(csv_path, low_memory=False)

        # Create the table and load data into it
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
            with engine.connect() as connection:
                df.to_sql(name=table_name, con=connection, if_exists='replace', index=False)
            print(f"Table '{table_name}' created and data loaded from file {csv_path}")

    def table_exists(table_name):
        # Function to check if a table exists in the database
        query = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = :table_name
            );
        """)
        with engine.connect() as connection:
            result = connection.execute(query, {'table_name': table_name}).scalar()
        return result

    # Iterate through all subdirectories and CSV files
    for root, _, files in os.walk(main_path):
        for file in files:
            if file.endswith('.csv'):
                table_name = os.path.splitext(file)[0]
                csv_path = os.path.join(root, file)
                
                # Check if table already exists
                if not table_exists(table_name):
                    create_table_from_csv(csv_path, table_name)
                else:
                    print(f"Table '{table_name}' already exists. Skipping file {csv_path}.")

    print("All new data successfully loaded into the database.")