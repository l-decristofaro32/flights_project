CREATE TABLE DimAirlines (
    airline_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    airline_name VARCHAR(255),
    country VARCHAR(255)
);

-- drop table DimAirlines;
-- ALTER TABLE DimAirlines1 RENAME TO DimAirlines;

CREATE TABLE DimAirports (
    airport_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    iata_code VARCHAR(3),
    airport_name VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255)
);

-- drop table DimAirports;
-- ALTER TABLE DimAirports1 RENAME TO DimAirports;

CREATE TABLE DimDate (
    date_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    date_flight VARCHAR(255),
    day_of_week VARCHAR(50),
    month VARCHAR(50),
    year INT
);

-- drop table DimDate;
-- ALTER TABLE DimDate1 RENAME TO DimDate;