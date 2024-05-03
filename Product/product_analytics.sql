WITH weekday_distribution AS (WITH user_first_touch_cte AS (
  SELECT
    user_pseudo_id,
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS event_date,
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_touch_timestamp
  FROM
    `turing_data_analytics.raw_events`
  GROUP BY
    user_pseudo_id
),
user_first_purchase_cte AS (
  SELECT
    user_pseudo_id,
    purchase_revenue_in_usd AS value,
    country,
    category AS device,
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS event_date,
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_purchase_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY TIMESTAMP_MICROS(event_timestamp)) AS purchase_row_number
  FROM
    `turing_data_analytics.raw_events`
  WHERE
    event_name = 'purchase'
  GROUP BY
    user_pseudo_id,
    purchase_revenue_in_usd,
    event_timestamp,
    country,
    device
)
SELECT
  touch_cte.user_pseudo_id,
  touch_cte.event_date AS purchase_date,
  ufp.value AS value,
  ufp.device,
  touch_cte.first_touch_timestamp,
  ufp.first_purchase_timestamp,
  DATETIME_DIFF(ufp.first_purchase_timestamp, touch_cte.first_touch_timestamp, MINUTE) AS time_to_purchase_minutes
FROM
  user_first_touch_cte touch_cte
JOIN (
  SELECT
    user_pseudo_id,
    value,
    device,
    event_date,
    first_purchase_timestamp
  FROM
    user_first_purchase_cte
  WHERE
    purchase_row_number = 1
) ufp ON
  touch_cte.user_pseudo_id = ufp.user_pseudo_id
  AND touch_cte.event_date = ufp.event_date
ORDER BY
  purchase_date)
SELECT wkd.user_pseudo_id,
  wkd.purchase_date,
  wkd.value,
  wkd.device,
  wkd.first_touch_timestamp,
  wkd.first_purchase_timestamp,
  wkd.time_to_purchase_minutes,
CASE EXTRACT(DAYOFWEEK FROM wkd.first_purchase_timestamp)
        WHEN 1 THEN '7. Sunday' 
        WHEN 2 THEN '1. Monday' 
        WHEN 3 THEN '2. Tuesday' 
        WHEN 4 THEN '3. Wednesday' 
        WHEN 5 THEN '4. Thursday' 
        WHEN 6 THEN '5. Friday' 
        WHEN 7 THEN '6. Saturday' 
    END AS week_day,
FROM weekday_distribution wkd

