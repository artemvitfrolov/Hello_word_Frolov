- Таблица для сырых логов событий
CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points Int32,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time)
TTL event_time + INTERVAL 30 DAY;

-- Агрегированная таблица
CREATE TABLE aggregated_events (
    event_date Date,
    event_type String,
    unique_users AggregateFunction(uniq, UInt32),
    spent_points AggregateFunction(sum, Int32),
    actions_count AggregateFunction(count, UInt32),
    retention_flag Bool
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- Материализованное представление
CREATE MATERIALIZED VIEW mv_aggregate_events TO aggregated_events AS
SELECT
    toDate(event_time) as event_date,
    event_type,
    uniqState(user_id) AS unique_users,
    sumState(points) AS spent_points,
    countState(*) AS actions_count,
    false AS retention_flag
FROM user_events
GROUP BY event_date, event_type;

-- Тестовые данные
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- Запрос для расчета Retention
WITH first_logins AS (
    SELECT user_id, min(toDate(event_time)) AS first_event_date
    FROM user_events
    GROUP BY user_id
),

seven_days_later AS (
    SELECT first_logins.user_id, first_logins.first_event_date, 
           first_logins.first_event_date + INTERVAL 7 DAY AS seven_days_after_first_visit
    FROM first_logins
),

events_within_seven_days AS (
    SELECT seven_days_later.user_id, COUNT(*) AS events_in_7_days
    FROM seven_days_later
    INNER JOIN user_events ON seven_days_later.user_id = user_events.user_id AND 
                            toDate(user_events.event_time) BETWEEN seven_days_later.first_event_date AND seven_days_later.seven_days_after_first_visit
    GROUP BY seven_days_later.user_id
)

SELECT 
    count(first_logins.user_id) AS total_users_day_0,
    countIf(events_within_seven_days.events_in_7_days >= 1) AS returned_in_7_days,
    round((countIf(events_within_seven_days.events_in_7_days >= 1) / count(first_logins.user_id)), 2) AS retention_7d_percent
FROM first_logins LEFT JOIN events_within_seven_days USING (user_id);

-- Быстрая аналитика по дням
SELECT 
    event_date,
    groupArray(event_type) AS types,
    groupArray(finalizeAggregation(unique_users)) AS unique_users_per_type,
    groupArray(finalizeAggregation(spent_points)) AS spent_points_per_type,
    groupArray(finalizeAggregation(actions_count)) AS actions_count_per_type
FROM aggregated_events
GROUP BY event_date
ORDER BY event_date ASC;
