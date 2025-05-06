-- Bronze table
CREATE TABLE IF NOT EXISTS bronze (
    trip_id text,
    taxi_id text,
    trip_start_timestamp timestamp,
    trip_end_timestamp timestamp,
    trip_seconds int,
    trip_miles float,
    pickup_census_tract text,
    dropoff_census_tract text,
    pickup_community_area int,
    dropoff_community_area int,
    fare float,
    tips float,
    tolls float,
    extras float,
    trip_total float,
    payment_type text,
    company text,
    pickup_latitude float,
    pickup_longitude float,
    pickup_location text,
    dropoff_latitude float,
    dropoff_longitude float,
    dropoff_location text,
    PRIMARY KEY (trip_id)
);

-- Silver table
CREATE TABLE IF NOT EXISTS silver (
    trip_id text,
    trip_start_date date,
    trip_start_time time,
    trip_end_date date,
    trip_end_time time,
    trip_seconds int,
    trip_miles float,
    pickup_community_area int,
    dropoff_community_area int,
    fare float,
    tips float,
    tolls float,
    extras float,
    trip_total float,
    payment_type text,
    PRIMARY KEY ((pickup_community_area, trip_start_date), trip_id)
);

-- index on silver
CREATE INDEX silver_trip_start_date_idx ON silver (trip_start_date);

-- Gold area revenue table
CREATE TABLE IF NOT EXISTS gold_area_1 (
    trip_start_date date,
    pickup_community_area int,
    total_fare float,
    total_tips float,
    total_trip_total float,
    avg_trip_total float,
    trip_count int,
    PRIMARY KEY ((trip_start_date), pickup_community_area)
);

-- Gold duration table
CREATE TABLE IF NOT EXISTS gold_duration (
    duration_label text,
    trip_count int,
    PRIMARY KEY (duration_label)
);

-- Gold payment table
CREATE TABLE IF NOT EXISTS gold_payment (
    payment_type text,
    total_fare float,
    total_tips float,
    avg_trip_total float,
    trip_count int,
    PRIMARY KEY (payment_type)
);

-- Gold time category table
CREATE TABLE IF NOT EXISTS gold_time_category (
    trip_hour int,
    time_category text,
    trip_count int,
    avg_fare float,
    avg_tips float,
    avg_trip_miles float,
    PRIMARY KEY ((time_category), trip_hour)
);

