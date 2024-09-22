import pandas as pd
import numpy as np
from faker import Faker
import os
from datetime import datetime

# Initialize Faker
fake = Faker()

# Number of records
num_records = np.random.randint(100000)

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

# Generate data
data = {
    "flight_id": [fake.uuid4() for _ in range(num_records)],
    "airline": [fake.company() for _ in range(num_records)],
    "departure_airport": [np.random.choice(airport_codes) for _ in range(num_records)],
    "arrival_airport": [np.random.choice(airport_codes) for _ in range(num_records)],
}

# Add columns based on the chosen airports
data["departure_country"] = [iata_to_country[airport] for airport in data["departure_airport"]]
data["arrival_country"] = [iata_to_country[airport] for airport in data["arrival_airport"]]
data["city_departure"] = [iata_to_city[airport] for airport in data["departure_airport"]]
data["city_arrival"] = [iata_to_city[airport] for airport in data["arrival_airport"]]
data["airport_name_departure"] = [iata_to_name[airport] for airport in data["departure_airport"]]
data["airport_name_arrival"] = [iata_to_name[airport] for airport in data["arrival_airport"]]

# Add other flight-related columns
data.update({
    "scheduled_departure": [fake.date_time_this_year() for _ in range(num_records)],
    "scheduled_arrival": [fake.date_time_this_year() for _ in range(num_records)],
    "actual_departure": [fake.date_time_this_year() for _ in range(num_records)],
    "actual_arrival": [fake.date_time_this_year() for _ in range(num_records)],
    "flight_duration": [np.random.randint(30, 600) for _ in range(num_records)],  # in minutes
    "status": [np.random.choice(["On Time", "Delayed", "Cancelled"]) for _ in range(num_records)]
})

# Create DataFrame
flights_df = pd.DataFrame(data)

# Adjust ScheduledArrival to be after ScheduledDeparture and ActualArrival to be after ActualDeparture
flights_df["scheduled_arrival"] = flights_df.apply(
    lambda row: row["scheduled_departure"] + pd.Timedelta(minutes=row["flight_duration"]), axis=1)

# Generate ActualDeparture ensuring it's not before ScheduledDeparture
flights_df["actual_departure"] = flights_df.apply(
    lambda row: row["scheduled_departure"] + pd.Timedelta(minutes=np.random.randint(0, 120)), axis=1)

flights_df["actual_arrival"] = flights_df.apply(
    lambda row: row["actual_departure"] + pd.Timedelta(minutes=row["flight_duration"] + np.random.randint(-15, 60)), axis=1)

# Extract the date (YYYY-MM-DD) from ScheduledDeparture and store it as a new column
flights_df["date_flight"] = pd.to_datetime(flights_df["scheduled_departure"].dt.date)
flights_df["day_of_week"] = flights_df["scheduled_departure"].dt.strftime("%A")
flights_df["month"] = flights_df["scheduled_departure"].dt.strftime("%B")
flights_df["year"] = flights_df["scheduled_departure"].dt.strftime("%Y")

# Save the DataFrame to a CSV file
output_path_folder = f"./flights_yyyymmdd"
os.makedirs(output_path_folder, exist_ok=True)

output_path = os.path.join(output_path_folder, f"flights_dataset_{datetime.now().strftime('%Y%m%d')}.csv")
flights_df.to_csv(output_path, index=False, mode="w")

print("Flight dataset generated and saved successfully!")