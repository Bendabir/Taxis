-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS e21 
WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS e21.facts_by_day (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,	
	PRIMARY KEY ((year, month, day), hour, minutes, seconds, trip_id)
);

CREATE TABLE IF NOT EXISTS e21.facts_by_week_day (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,	
	PRIMARY KEY ((year, week, day_of_week), hour, minutes, seconds, trip_id)
);

CREATE TABLE IF NOT EXISTS e21.facts_by_day_type (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,	
	PRIMARY KEY ((year, month, day_type), hour, minutes, seconds, trip_id)
);

CREATE TABLE IF NOT EXISTS e21.facts_by_start_zone (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,
	PRIMARY KEY ((start_zone_lon, start_zone_lat), year, month, day, minutes, seconds, trip_id)
);

CREATE TABLE IF NOT EXISTS e21.facts_by_arrival_zone (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,	
	PRIMARY KEY ((arrival_zone_lon, arrival_zone_lat), year, month, day, minutes, seconds, trip_id)
);

CREATE TABLE IF NOT EXISTS e21.facts_by_taxi (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,	
	PRIMARY KEY (taxi_id, year, month, day, minutes, seconds, trip_id)
);

CREATE TABLE IF NOT EXISTS e21.facts_by_distance (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,
	truncated_distance INT,	
	PRIMARY KEY (truncated_distance, distance, year, month, day, minutes, seconds, trip_id)
);

CREATE TABLE IF NOT EXISTS e21.facts_by_duration (
	start_zone_lon FLOAT,
	start_zone_lat FLOAT,
	arrival_zone_lon FLOAT,
	arrival_zone_lat FLOAT,
	taxi_id INT,
	trip_id BIGINT,
	call_type TEXT,
	origin_call TEXT,
	origin_stand TEXT,	
	year INT,
	month INT,
	day INT,
	hour INT,
	minutes INT,
	seconds INT,
	day_of_week INT,
	week INT,
	period TEXT,
	day_type TEXT,	
	start_lon FLOAT,
	start_lat FLOAT,
	arrival_lon FLOAT,
	arrival_lat FLOAT,
	duration INT,
	distance FLOAT,
	truncated_duration INT,	
	PRIMARY KEY (truncated_duration, duration, year, month, day, minutes, seconds, trip_id)
);


-- -- Building time dimension
-- CREATE TABLE IF NOT EXISTS e21.time_by_year (
-- 	year INT, -- Partition key
-- 	month INT, -- Clusters
-- 	day INT, -- Clusters
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT,
-- 	week INT,
-- 	hour INT, -- Clusters
-- 	minutes INT,
-- 	period TEXT,
-- 	day_type TEXT,
-- 	PRIMARY KEY (year, month, day, hour)
-- );

-- CREATE TABLE IF NOT EXISTS e21.time_by_month (
-- 	year INT, -- Clusters 
-- 	month INT, -- Partition key
-- 	day INT, -- Clusters
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT,
-- 	week INT,
-- 	hour INT, -- Clusters
-- 	minutes INT,
-- 	period TEXT,
-- 	day_type TEXT,
-- 	PRIMARY KEY (month, year, day, hour)
-- );

-- CREATE TABLE IF NOT EXISTS e21.time_by_day (
-- 	year INT, -- Clusters 
-- 	month INT, -- Clusters
-- 	day INT, -- Partition key
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT,
-- 	week INT,
-- 	hour INT, -- Clusters
-- 	minutes INT,
-- 	period TEXT,
-- 	day_type TEXT,	
-- 	PRIMARY KEY (day, month, year, hour)
-- );

-- CREATE TABLE IF NOT EXISTS e21.time_by_hour (
-- 	year INT, -- Clusters 
-- 	month INT, -- Clusters
-- 	day INT, -- Clusters
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT,
-- 	week INT,
-- 	hour INT, -- Partition key
-- 	minutes INT,
-- 	period TEXT,
-- 	day_type TEXT,
-- 	PRIMARY KEY (hour, day, month, year)
-- );

-- CREATE TABLE IF NOT EXISTS e21.time_by_day_of_week (
-- 	year INT, 
-- 	month INT,
-- 	day INT,
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT, -- Partition key
-- 	week INT, -- Clusters
-- 	hour INT, -- Clusters
-- 	minutes INT,
-- 	period TEXT,
-- 	day_type TEXT,
-- 	PRIMARY KEY (day_of_week, week, hour)
-- );

-- CREATE TABLE IF NOT EXISTS e21.time_by_week (
-- 	year INT, 
-- 	month INT,
-- 	day INT,
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT, -- Clusters
-- 	week INT, -- Partition key
-- 	hour INT, -- Clusters
-- 	minutes INT,
-- 	period TEXT,
-- 	day_type TEXT,
-- 	PRIMARY KEY (week, day_of_week, hour)
-- );

-- CREATE TABLE IF NOT EXISTS e21.time_by_period (
-- 	year INT, -- Clusters
-- 	month INT, -- Clusters
-- 	day INT, -- Clusters
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT,
-- 	week INT,
-- 	hour INT,
-- 	minutes INT,
-- 	period TEXT, -- Partition key
-- 	day_type TEXT,
-- 	PRIMARY KEY (period, day, month, year)
-- );

-- CREATE TABLE IF NOT EXISTS e21.time_by_day_type (
-- 	year INT, -- Clusters
-- 	month INT, -- Clusters
-- 	day INT, -- Clusters
-- 	date_timestamp TIMESTAMP,
-- 	day_of_week INT,
-- 	week INT,
-- 	hour INT,
-- 	minutes INT,
-- 	period TEXT, 
-- 	day_type TEXT, -- Partition key
-- 	PRIMARY KEY (day_type, day, month, year)
-- );

-- -- Building location dimension
-- CREATE TABLE IF NOT EXISTS e21.locations_by_zone (
-- 	lon FLOAT,
-- 	lat FLOAT,
-- 	zone_lon FLOAT, -- Partition key
-- 	zone_lat FLOAT, -- Partition key
-- 	PRIMARY KEY ((zone_lon, zone_lat), lon, lat)
-- );

-- CREATE TABLE IF NOT EXISTS e21.locations_by_lat (
-- 	lon FLOAT,
-- 	lat FLOAT,
-- 	zone_lat FLOAT, -- Partition key
-- 	PRIMARY KEY (zone_lat, lat)
-- );

-- CREATE TABLE IF NOT EXISTS e21.locations_by_lon (
-- 	lon FLOAT,
-- 	lat FLOAT,
-- 	zone_lon FLOAT, -- Partition key
-- 	PRIMARY KEY (zone_lon, lon)
-- );

-- -- Building trips dimension
-- CREATE TABLE IF NOT EXISTS e21.trips_by_taxi (
-- 	trip_id BIGINT,
-- 	taxi_id INT, -- Partition key
-- 	call_type TEXT,
-- 	origin_call TEXT,
-- 	origin_stand TEXT,
-- 	PRIMARY KEY (taxi_id, trip_id)
-- );

-- CREATE TABLE IF NOT EXISTS e21.trips_by_call_type (
-- 	trip_id BIGINT,
-- 	taxi_id INT,
-- 	call_type TEXT, -- Partition key
-- 	origin_call TEXT,
-- 	origin_stand TEXT,
-- 	PRIMARY KEY (call_type, trip_id)
-- );

-- CREATE TABLE IF NOT EXISTS e21.trips_by_origin_call (
-- 	trip_id BIGINT,
-- 	taxi_id INT,
-- 	call_type TEXT,
-- 	origin_call TEXT, -- Partition key
-- 	origin_stand TEXT,
-- 	PRIMARY KEY (origin_call, trip_id)
-- );

-- CREATE TABLE IF NOT EXISTS e21.trips_by_origin_stand (
-- 	trip_id BIGINT,
-- 	taxi_id INT,
-- 	call_type TEXT,
-- 	origin_call TEXT,
-- 	origin_stand TEXT, -- Partition key
-- 	PRIMARY KEY (origin_stand, trip_id)
-- );


-- -- Building facts tables
-- CREATE TABLE IF NOT EXISTS e21.facts_by_duration (
-- 	trip_timestamp TIMESTAMP,
-- 	trip_id BIGINT,
-- 	lon_start FLOAT,
-- 	lat_start FLOAT,
-- 	lon_end FLOAT,
-- 	lat_end FLOAT,
-- 	duration INT,
-- 	PRIMARY KEY (duration, trip_id)
-- );

-- CREATE TABLE IF NOT EXISTS e21.facts_by_distance (
-- 	trip_timestamp TIMESTAMP,
-- 	trip_id BIGINT,
-- 	distance DOUBLE,
-- 	lon_start FLOAT,
-- 	lat_start FLOAT,
-- 	lon_end FLOAT,
-- 	lat_end FLOAT,	
-- 	PRIMARY KEY (distance, trip_id)
-- );





-- CREATE TABLE e21.trips_by_taxi (
-- 	taxi_id INT,
-- 	trip_id TEXT,
-- 	trip_timestamp TIMESTAMP,
-- 	PRIMARY KEY (taxi_id, trip_timestamp)
-- ) WITH CLUSTERING ORDER BY (trip_timestamp ASC);

-- CREATE TABLE e21.trips_by_call_type (
-- 	call_type TEXT,
-- 	trip_id TEXT,
-- 	trip_timestamp TIMESTAMP,
-- 	PRIMARY KEY (call_type, trip_timestamp)
-- ) WITH CLUSTERING ORDER BY (trip_timestamp ASC);








-- Temps(#timestamp, date, jour, mois, année, jourDeLaSemaine, semaine, heure, minutes, période, typeDeJour)
-- Lieux(#lon, #lat, zone)
-- Trajet(#id, taxi, typeDAppel, origineDAppel, standDOrigine, duree, distance)

-- Create table
-- CREATE TABLE IF NOT EXISTS e21.locations (
-- 	lon FLOAT,
-- 	lat FLOAT,
-- 	zone INT,
-- 	PRIMARY KEY (lon, lat)
-- );

-- CREATE TABLE IF NOT EXISTS e21.trips (
-- 	id INT,
-- 	taxi INT,
-- 	callType TEXT,
-- 	originCall INT,
-- 	originStand INT,
-- 	duration INT,
-- 	distance INT,
-- 	PRIMARY KEY (id)
-- );

-- CREATE TABLE IF NOT EXISTS e21.dates (
-- 	tmstp INT, -- date_timestamp TIMESTAMP
-- 	date TEXT,
-- 	day INT,
-- 	month INT,
-- 	year INT,
-- 	dayOfWeek INT,
-- 	weekOfYear INT,
-- 	hour INT,
-- 	minutes INT,
-- 	period TEXT,
-- 	dayType TEXT,
-- 	PRIMARY KEY (tmstp)
-- );

-- CREATE TABLE IF NOT EXISTS e21.facts (
-- 	duration,
-- 	distance	
-- );



-- CREATE TABLE crossfit_gyms_by_location ( 
--     country_code text, 	-- Partition key
--     state_province text, -- Clustering key
--     city text, 			-- Clustering key
--     gym_name text,		-- Clustering key
--     PRIMARY KEY (country_code, state_province, city, gym_name) 
-- ) WITH CLUSTERING ORDER BY (state_province DESC, city ASC, gym_name ASC); 