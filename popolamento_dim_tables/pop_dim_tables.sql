CREATE TABLE stagingflights (
    flight_id UUID,  -- Correct data type for FLIGHTID
    airline VARCHAR(255),
    departure_airport VARCHAR(255),
    arrival_airport VARCHAR(255),
    departure_country VARCHAR(255),
    arrival_country VARCHAR(255),
    city_departure VARCHAR(255),
    city_arrival VARCHAR(255),
    airport_name_departure VARCHAR(255),
    airport_name_arrival VARCHAR(255),
    scheduled_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    actual_departure TIMESTAMP,
    actual_arrival TIMESTAMP,
    flight_duration INT,
    status VARCHAR(255),
    date_flight DATE,
    day_of_week VARCHAR(255),
    month VARCHAR(255),
    year INT
);

CREATE OR REPLACE FUNCTION trg_insert_staging_flights_func()
RETURNS TRIGGER AS $$
BEGIN
    -- Inserimento in DimAirlines con ON CONFLICT per evitare duplicati
    INSERT INTO DimAirlines (airline_name, country)
    VALUES (new.airline, new.departure_country)
    ON CONFLICT (airline_name) DO NOTHING;

    -- Inserimento in DimAirports con ON CONFLICT
    -- Si inseriscono sia gli aeroporti di partenza che di arrivo
    INSERT INTO DimAirports (iata_code, airport_name, city, country)
    VALUES (new.departure_airport, new.airport_name_departure, new.city_departure, new.departure_country)
    ON CONFLICT (IATACode) DO NOTHING;

    INSERT INTO DimAirports (iata_code, airport_name, city, country)
    VALUES (new.arrival_airport, new.airport_name_arrival, new.city_arrival, new.arrival_country)
    ON CONFLICT (iata_code) DO NOTHING;

    -- Inserimento in DimDate con ON CONFLICT
    INSERT INTO DimDate (date_flight, day_of_week, month, year)
    VALUES (new.date_flight, new.day_of_week, new.month, new.year)
    ON CONFLICT (date_flight) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_insert_staging_flights
AFTER INSERT ON StagingFlights
FOR EACH ROW
EXECUTE FUNCTION trg_insert_staging_flights_func();
