CREATE SCHEMA IF NOT EXISTS application_db;

SET search_path TO application_db, public;

CREATE TABLE IF NOT EXISTS application_db.orders
(
    order_id INT,
    city_code INT NOT NULL,
    PRIMARY KEY(order_id)
);

CREATE TABLE IF NOT EXISTS application_db.customer_courier_chat_messages
(
    sender_app_type VARCHAR(30),
    customer_id INT NOT NULL,
    from_id INT NOT NULL,
    to_id INT NOT NULL,
    chat_started_by_message BOOLEAN NOT NULL,
    order_id INT NOT NULL,
    order_stage VARCHAR(30),
    courier_id INT NOT NULL,
    message_sent_time TIMESTAMP,
    CONSTRAINT fk_order_id
      FOREIGN KEY(order_id) 
	  REFERENCES orders(order_id)
);
