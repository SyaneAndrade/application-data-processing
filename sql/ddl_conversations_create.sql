CREATE TABLE IF NOT EXISTS application_db.customer_courier_conversations AS WITH join_orders_messages AS(
	SELECT orders.order_id,
		orders.city_code,
		messages.sender_app_type,
		messages.customer_id,
		messages.from_id,
		messages.to_id,
		messages.chat_started_by_message,
		messages.order_stage,
		messages.courier_id,
		messages.message_sent_time
	FROM application_db.customer_courier_chat_messages as messages
		LEFT JOIN application_db.orders as orders ON messages.order_id = orders.order_id
),
add_row_number_conversation AS (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY order_id
			ORDER BY message_sent_time ASC
		) AS rn_order_messages
	FROM join_orders_messages
),
add_row_number_conversation_desc AS (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY order_id
			ORDER BY message_sent_time DESC
		) AS rn_order_messages_desc
	FROM join_orders_messages
),
added_row_number_courier AS (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY order_id
			ORDER BY message_sent_time ASC
		) AS rn_first_courier
	FROM join_orders_messages
	WHERE from_id = courier_id
),
first_courier_message AS (
	SELECT order_id,
		message_sent_time
	FROM added_row_number_courier
	WHERE rn_first_courier = 1
),
added_row_number_customer AS (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY order_id
			ORDER BY message_sent_time ASC
		) AS rn_first_customer
	FROM join_orders_messages
	WHERE from_id = customer_id
),
first_customer_message AS (
	SELECT order_id,
		message_sent_time
	FROM added_row_number_customer
	WHERE rn_first_customer = 1
),
count_messages_courier AS (
	SELECT order_id,
		count(1) as num_messages_courier
	FROM added_row_number_courier
	GROUP BY order_id
),
count_messages_customer AS (
	SELECT order_id,
		count(1) as num_messages_customer
	FROM added_row_number_customer
	GROUP BY order_id
),
first_message_by AS(
	SELECT order_id,
		CASE
			WHEN from_id = courier_id THEN 'courier'
			ELSE 'customer'
		END AS first_message_by,
		message_sent_time AS conversation_started_at
	FROM add_row_number_conversation
	WHERE rn_order_messages = 1
),
last_message_time_and_stage AS (
	SELECT order_id,
		message_sent_time AS last_message_time,
		order_stage AS last_message_order_stage
	FROM add_row_number_conversation_desc
	WHERE rn_order_messages_desc = 1
),
response_first_message AS (
	SELECT order_id,
		message_sent_time AS response_time
	FROM add_row_number_conversation
	WHERE rn_order_messages = 2
),
customer_courier_messages AS (
	SELECT join_orders_messages.order_id AS order_id,
		join_orders_messages.city_code AS city_code,
		first_courier_message.message_sent_time AS first_courier_message,
		first_customer_message.message_sent_time AS first_customer_message,
		count_messages_courier.num_messages_courier AS num_messages_courier,
		count_messages_customer.num_messages_customer AS num_messages_customer,
		first_message_by.first_message_by AS first_message_by,
		first_message_by.conversation_started_at AS conversation_started_at,
		EXTRACT (
			EPOCH
			FROM (
					response_first_message.response_time - first_message_by.conversation_started_at
				)
		) AS first_responsetime_delay_seconds,
		last_message_time_and_stage.last_message_time AS last_message_time,
		last_message_time_and_stage.last_message_order_stage AS last_message_order_stage
	FROM join_orders_messages
		LEFT JOIN first_courier_message ON join_orders_messages.order_id = first_courier_message.order_id
		LEFT JOIN first_customer_message ON join_orders_messages.order_id = first_customer_message.order_id
		LEFT JOIN count_messages_courier ON join_orders_messages.order_id = count_messages_courier.order_id
		LEFT JOIN count_messages_customer ON join_orders_messages.order_id = count_messages_customer.order_id
		LEFT JOIN first_message_by ON join_orders_messages.order_id = first_message_by.order_id
		LEFT JOIN last_message_time_and_stage ON join_orders_messages.order_id = last_message_time_and_stage.order_id
		LEFT JOIN response_first_message ON join_orders_messages.order_id = response_first_message.order_id
)
SELECT order_id,
	city_code,
	first_courier_message,
	first_customer_message,
	num_messages_courier,
	num_messages_customer,
	first_message_by,
	conversation_started_at,
	first_responsetime_delay_seconds,
	last_message_time,
	last_message_order_stage
FROM customer_courier_messages
GROUP BY (
		order_id,
		city_code,
		first_courier_message,
		first_customer_message,
		num_messages_courier,
		num_messages_customer,
		first_message_by,
		conversation_started_at,
		first_responsetime_delay_seconds,
		last_message_time,
		last_message_order_stage
	);