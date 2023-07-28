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

select count(*) from (
select user_id, count(user_id) as total from events group by user_id having total > 20 order by user_id desc) total;

select * from email_events;
select * from users;
select distinct state from users;

SELECT
    WEEK(occurred_at) AS week_number,
    COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY week_number
ORDER BY  week_number;

SELECT
extract (week from occurred_at) as week_number,
count(distinct user_id) as number_of_users
FROM
tutorial.yammer_events
group by
week_number;

use `operation & metric analytics`;


