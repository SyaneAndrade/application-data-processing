CREATE SCHEMA IF NOT EXISTS application_db;

USE application_db;

CREATE TABLE IF NOT EXISTS orders
(
    order_id INT GENERATED ALWAYS AS IDENTITY,
    city_code INT NOT NULL,
    PRIMARY KEY(order_id)
);