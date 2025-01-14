CREATE TABLE IF NOT EXISTS cfproject.prices(
            prices_id INT PRIMARY key,
            price NUMERIC,
            departure VARCHAR(50),
            arrival VARCHAR(50),
            flight_No VARCHAR(50),
            requested_at timestamp,
            airline_id INT,
            constraint fk_airlines foreign key(airline_id) references cfproject.airlines(airline_id)
        );