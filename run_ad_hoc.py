import pandas as pd
import numpy as np
from faker import Faker
import os
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Number of records per day
num_records_per_day = np.random.randint(100000) 

# Path to the airport codes CSV
path_airport_codes = os.path.join(os.getcwd(), "csv_airport", "airports_codes.csv")
print(path_airport_codes)

# Check if the file exists
if os.path.exists(path_airport_codes):
    # Load airport codes
    airport_df = pd.read_csv(path_airport_codes)
    
    # Create separate dictionaries mapping IATA to Country, City, and Name
    iata_to_country = dict(zip(airport_df["IATA"], airport_df["Country"]))
    iata_to_city = dict(zip(airport_df["IATA"], airport_df["City"]))
    iata_to_name = dict(zip(airport_df["IATA"], airport_df["Name"]))
    
    airport_codes = airport_df["IATA"].values
else:
    raise FileNotFoundError(f"The file {path_airport_codes} does not exist.")

# Folder to save the generated CSV files
output_path_folder = f"./flights_yyyymmdd"
os.makedirs(output_path_folder, exist_ok=True)

# Start and end date for flight generation
start_date = datetime(2024, 7, 16)
end_date = datetime(2024, 9, 6)

# Generate data for each day in the range
current_date = start_date
while current_date <= end_date:
    # Generate data for the current day
    data = {
        "flight_id": [fake.uuid4() for _ in range(num_records_per_day)],
        "airline": [fake.company() for _ in range(num_records_per_day)],
        "departure_airport": [np.random.choice(airport_codes) for _ in range(num_records_per_day)],
        "arrival_airport": [np.random.choice(airport_codes) for _ in range(num_records_per_day)],
    }

    # Add columns based on the chosen airports
    data["departure_country"] = [iata_to_country[airport] for airport in data["departure_airport"]]
    data["arrival_country"] = [iata_to_country[airport] for airport in data["arrival_airport"]]
    data["city_departure"] = [iata_to_city[airport] for airport in data["departure_airport"]]
    data["city_arrival"] = [iata_to_city[airport] for airport in data["arrival_airport"]]
    data["airport_name_departure"] = [iata_to_name[airport] for airport in data["departure_airport"]]
    data["airport_name_arrival"] = [iata_to_name[airport] for airport in data["arrival_airport"]]

    # Set scheduled and actual times for the flights of the current day
    scheduled_times = [current_date + timedelta(minutes=np.random.randint(0, 1440)) for _ in range(num_records_per_day)]
    data.update({
        "scheduled_departure": scheduled_times,
        "flight_duration": [np.random.randint(30, 600) for _ in range(num_records_per_day)],  # in minutes
        "status": [np.random.choice(["On Time", "Delayed", "Cancelled"]) for _ in range(num_records_per_day)]
    })

    # Adjust SCHEDULEDARRIVAL based on FLIGHTDURATION
    data["scheduled_arrival"] = [data["scheduled_departure"][i] + timedelta(minutes=data["flight_duration"][i]) for i in range(num_records_per_day)]

    # Generate actual departure and arrival times (ensure actual times are logical)
    data["actual_departure"] = [data["scheduled_departure"][i] + timedelta(minutes=np.random.randint(0, 120)) for i in range(num_records_per_day)]
    data["actual_arrival"] = [data["actual_departure"][i] + timedelta(minutes=data["flight_duration"][i] + np.random.randint(-15, 60)) for i in range(num_records_per_day)]

    # Create DataFrame for the current day's data
    flights_df = pd.DataFrame(data)

    # Extract the date (YYYY-MM-DD) from ScheduledDeparture and store it as a new column
    flights_df["date_flight"] = pd.to_datetime(flights_df["scheduled_departure"]).dt.date
    flights_df["day_of_week"] = pd.to_datetime(flights_df["scheduled_departure"]).dt.strftime("%A")
    flights_df["month"] = pd.to_datetime(flights_df["scheduled_departure"]).dt.strftime("%B")
    flights_df["year"] = pd.to_datetime(flights_df["scheduled_departure"]).dt.strftime("%Y")

    # Save the DataFrame to a CSV file named after the current date
    output_path = os.path.join(output_path_folder, f"flights_dataset_{current_date.strftime('%Y%m%d')}.csv")
    flights_df.to_csv(output_path, index=False, mode="w")
    
    print(f"Flight dataset for {current_date.strftime('%Y-%m-%d')} generated and saved successfully!")

    # Move to the next day
    current_date += timedelta(days=1)
