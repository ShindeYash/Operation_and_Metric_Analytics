use temp_db;
select * from events;
select count(temp_created_at) from events;
truncate table events;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv' into table events fields terminated by ','  ignore 1 lines ;

alter table events add column temp_created_at datetime;

SET SQL_SAFE_UPDATES = 0;


UPDATE events SET temp_created_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');

alter table events drop column occurred_at;

alter table events change column temp_created_at occurred_at datetime;

SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY week_number
ORDER BY  week_number ;

with signup as(
select occurred_at, event_name from events where event_name = "complete_signup")

SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(event_name) as total_signup
FROM signup
GROUP BY week_number
ORDER BY  week_number;