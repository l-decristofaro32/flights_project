import pandas as pd
import os

# URL del dataset OpenFlights
url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat'

# Leggi il dataset
airport_df = pd.read_csv(url, header=None, names=[
    "AirportID", "Name", "City", "Country", "IATA", "ICAO", "Latitude", 
    "Longitude", "Altitude", "Timezone", "DST", "TzDatabaseTimeZone", 
    "Type", "Source"])

# Filtra le colonne che ci interessano: IATA, Country, Name e City
airport_codes_df = airport_df[["IATA", "Country", "Name", "City"]].dropna()

# Elimina righe con codici IATA non validi (es. \N che rappresenta valori mancanti)
airport_codes_df = airport_codes_df[airport_codes_df['IATA'] != '\\N']

# Elimina eventuali duplicati
airport_codes_df = airport_codes_df.drop_duplicates()

os.path.join("csv_airport")

# Salva il DataFrame in un file CSV
output_path = './csv_airport/airports_codes.csv'
airport_codes_df.to_csv(output_path, index=False)

print(airport_codes_df)
