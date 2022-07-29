CREATE SCHEMA IF NOT EXISTS application_db;

CREATE TABLE IF NOT EXISTS customer_courier_chat_messages
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
