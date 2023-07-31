# Using the database

use `operation & metric analytics`;

### Job Data Analytics ###

# (1) Create an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.

select * from email_events;
select * from events;

select distinct event_name from events;

select * from job_data;
select * from users;

# to calculate number of jobs reviewed per hour we just have to calculate the total number of job_id from job_data and then divide it by 24*30

select count(job_id) from job_data;
select count(*) from job_data;

SELECT 
    COUNT(job_id) / (24 * 30) AS No_of_jobs_reviewed
FROM
    job_data;


    
# (2) Create an SQL query to calculate the 7-day rolling average of throughput. ((number of events per second)).

select * from job_data;

SELECT
        ds as date_of_review,
        COUNT(job_id) AS jobs_reviewed
    FROM job_data
    GROUP BY ds;


WITH jobs_by_date AS (
    SELECT
        ds as review_date,
        COUNT(job_id) AS jobs_reviewed
    FROM job_data
    GROUP BY ds
)
SELECT
    review_date,
    jobs_reviewed,
    AVG(jobs_reviewed) OVER (ORDER BY review_date) AS rolling_average_throughput_7days
FROM
    jobs_by_date
ORDER BY
    review_date;


# (3) Calculate the percentage share of each language in the last 30 days. 
# Write an SQL query to calculate the percentage share of each language over the last 30 days

select * from job_data;

select distinct language from job_data;

select job_id, language, count(language) as total
 from job_data
group by language;

with language_total as (
select job_id, language, 
count(language) as total 
from job_data
group by language)

SELECT 
    job_id,
    language,
    total,
    (total / (SELECT COUNT(*) FROM job_data)) * 100 AS percentage
FROM
    language_total;


# (4) Identify duplicate rows in the data. Write an SQL query to display duplicate rows from the job_data table.

select * from job_data;
select count(*) from job_data;


SELECT *
FROM job_data
WHERE (ds, job_id, actor_id, event, language, time_spent, org) IN (
    SELECT ds, job_id, actor_id, event, language, time_spent, org
    FROM job_data
    GROUP BY ds, job_id, actor_id, event, language, time_spent, org
    HAVING COUNT(*) > 1
);



### Investigating Metric Spike ###

# (1) Measure the activeness of users on a weekly basis. Write an SQL query to calculate the weekly user engagement.

select * from events;
select distinct event_type from events;

select * from email_events;
select * from users;
select distinct state from users;


SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY week_number
ORDER BY  week_number
;


# Date format changing
# table date type was not working so I have did following stuff to make occurred_at column usable again
use `operation & metric analytics`;

alter table `events`
modify occurred_at timestamp;

ALTER TABLE events add COLUMN new_date DATE;
UPDATE events SET new_date = STR_TO_DATE(occurred_at,'%d/%m/%Y') WHERE substring(occurred_at,3,1) = '/';

truncate table events;

select * from events;

alter table events
modify occurred_at date;

ALTER TABLE events DROP COLUMN new_date;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv' into table events fields terminated by ','  ignore 1 lines ;

-- Complited the date updation here ---

-- Stareted actual code to find the weekly user engagement.

select count(distinct user_id) from events;

SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY week_number
ORDER BY  week_number ;

-- Comleted the code ---

# (2) Analyze the growth of users over time for a product. Write an SQL query to calculate the user growth for the product.

select * from users;
select * from email_events;
select * from events;
select event_name, count(event_name) from events group by event_name;
select distinct language from users;
select count(*) from events;
select * from events;
select count(*) from events;

select count(*) from events;

SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY week_number
ORDER BY  week_number ;

SELECT
event_name,
    week(occurred_at) AS signup_date
FROM
    events
GROUP BY
    week(signup_date)
ORDER BY
    week(signup_date);

-- Actual Query Starts Here ---

with signup as(
select occurred_at, event_name from events where event_name = "complete_signup")
SELECT
    week_number,
    total_signup,
    SUM(total_signup) OVER (ORDER BY week_number) AS cumulative_signup
FROM
(SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(event_name) as total_signup
FROM signup
GROUP BY week_number
ORDER BY  week_number) as  weekly_signup;

-- Query and Problem ends here ------------

# (3) : Analyze the retention of users on a weekly basis after signing up for a product.
#  Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.

with activities as(
select occurred_at, event_name from events )
SELECT
    week_number,
    total_activities,
    SUM(total_activities) OVER (ORDER BY week_number) AS cumulative_activities
FROM
(SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(event_name) as total_activities
FROM activities
GROUP BY week_number
ORDER BY  week_number) as  weekly_activities;


# (4) : Weekly Engagement Per Device: Measure the activeness of users on a weekly basis per device. 
#  Write an SQL query to calculate the weekly engagement per device.

SELECT
week(occurred_at) as week_num,
device,
COUNT(distinct user_id) as no_of_users
FROM
events
where event_type = 'engagement'
GROUP by week_num, device
order by week_num;


# (5) Email Engagement Analysis:  Analyze how users are engaging with the email service & Write an SQL query to calculate the email engagement metrics.


SELECT
  100.0 * COUNT(CASE WHEN email = 'email_opened' THEN 1 END) / COUNT(CASE WHEN email = 'email_sent' THEN 1 END) AS email_opening_percentage,
  100.0 * COUNT(CASE WHEN email = 'email_clicked' THEN 1 END) / COUNT(CASE WHEN email = 'email_sent' THEN 1 END) AS email_clicking_percentage
FROM (
  SELECT
    *,
    CASE
      WHEN action IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 'email_sent'
      WHEN action IN ('email_open') THEN 'email_opened'
      WHEN action IN ('email_clickthrough') THEN 'email_clicked'
    END AS email
  FROM
    email_events
) email_action;



