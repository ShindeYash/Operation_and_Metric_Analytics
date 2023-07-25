USE `operation & metric analytics`;

drop table email_events;

SELECT * FROM email_events;
select count(*) from events;
select count(*) from email_events;
truncate table email_events;

create table email_events(
user_id int,
occurred_at varchar(30),
action varchar (30),
user_type int);

insert into email_events 

ALTER TABLE employees  
  MODIFY COLUMN emp_id int(5),  
  MODIFY COLUMN income VARCHAR(20);  

show variables like "secure_file_priv";

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv' into table email_events fields terminated by ','  ignore 1 lines ;

LOAD DATA INFILE 'C:/Users/Yash/Downloads/email_events.csv' into table events fields terminated by ',' optionally enclosed by '"' lines terminated by '\n' ignore 1 lines;