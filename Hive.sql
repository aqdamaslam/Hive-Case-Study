-- 1.	Load the data into a Hive table. Create an external table with the given schema and load the data into the table from a text file or HDFS path.

create external table if not exists car_insurance_calls (
    Id INT,
    Age INT,
    Job STRING,
    Marital STRING,
    Education STRING,
    `Default` STRING,
    Balance FLOAT,
    HHInsurance STRING,
    CarLoan STRING,
    Communication STRING,
    LastContactDay INT,
    LastContactMonth STRING,
    NoOfContacts INT,
    DaysPassed INT,
    PrevAttempts INT,
    Outcome STRING,
    CallStart STRING,
    CallEnd STRING,
    CarInsurance STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://path/to/your/data';


-- 1.	How many records are there in the dataset?

select count(*) from car_insurance_calls;

-- 2.	How many unique job categories are there?

select count(distinct(job)) as distinct_job from car_insurance_calls;

-- 3.	What is the age distribution of customers in the dataset? Provide a breakdown by age group: 18-30, 31-45, 46-60, 61+.
select case
when Age between 18 and 30 then '18-30'
when Age between 31 and 45 then '31-45'
when Age between 46 and 60 then '46-60'
else '61+'
end as age_group,
count(*) as total_count
from car_insurance_calls
group by case
when Age between 18 and 30 then '18-30'
when Age between 31 and 45 then '31-45'
when Age between 46 and 60 then '46-60'
else '61+'
end;

-- 4.	Count the number of records that have missing values in any field.

select count(*) from car_insurance_calls
where
    Id is null
    or Age is null
    or Job is null
    or Marital is null
    or Education is null
    or Default is null
    or Balance is null
    or HHInsurance is null
    or CarLoan is null
    or Communication is null
    or LastContactDay is null
    or LastContactMonth is null
    or NoOfContacts is null
    or DaysPassed is null
    or PrevAttempts is null
    or Outcome is null
    or CallStart is null
    or CallEnd is null
    or CarInsurance is null;

-- 5.	Determine the number of unique 'Outcome' values and their respective counts.

select Outcome, count(*) from car_insurance_calls group by Outcome;


-- 6.	Find the number of customers who have both a car loan and home insurance.

select count(*) from car_insurance_calls where CarLoan = 1 and HHInsurance = 1;


-- Aggregations

-- 1.	What is the average, minimum, and maximum balance for each job category?

select Job,
    avg(Balance) as Average_balance,
    min(Balance) as Minimum_balance,
    max(Balance) as Max_balance
from car_insurance_calls
group by Job;



-- 2.	Find the total number of customers with and without car insurance.

select CarInsurance, count(*) from car_insurance_calls group by CarInsurance;


-- 3.	Count the number of customers for each communication type.

select Communication, count(*) from car_insurance_calls group by Communication;


-- 4.	Calculate the sum of 'Balance' for each 'Communication' type.

select Communication, sum(Balance) as total_sum from car_insurance_calls group by Communication;


-- 5.	Count the number of 'PrevAttempts' for each 'Outcome' type.

select Outcome, count(PrevAttempts) as total_attempts from car_insurance_calls group by Outcome;


-- 6.	Calculate the average 'NoOfContacts' for people with and without 'CarInsurance'.

select CarInsurance, avg(NoOfContacts) as Averrage_Contacts from car_insurance_calls group by CarInsurance;



-- Partitioning and Bucketing

-- 1.	Create a partitioned table on 'Education' and 'Marital' status. Load data from the original table to this new partitioned table.

create external table if not exists car_insurance_calls_partitioned (
    Id INT,
    Age INT,
    Job STRING,
    `Default` STRING,
    Balance FLOAT,
    HHInsurance STRING,
    CarLoan STRING,
    Communication STRING,
    LastContactDay INT,
    LastContactMonth STRING,
    NoOfContacts INT,
    DaysPassed INT,
    PrevAttempts INT,
    Outcome STRING,
    CallStart STRING,
    CallEnd STRING,
    CarInsurance STRING
)
partitioned by (Education string, Marital string)
row format delimited
fields terminated by ','
stored as textfile;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table
car_insurance_calls_partitioned partition(Education,
Marital)
select Id,
    Age,
    Job,
    `Default`,
    Balance,
    HHInsurance,
    CarLoan,
    Communication,
    LastContactDay,
    LastContactMonth,
    NoOfContacts,
    DaysPassed,
    PrevAttempts,
    Outcome,
    CallStart,
    CallEnd,
    CarInsurance,
    Marital,
    Education
from car_insurance_calls;


-- 2.	Create a bucketed table on 'Age', bucketed into 4 groups (as per the age groups mentioned above). Load data from the original table into this bucketed table.

create table if not exists car_insurance_calls_buckted (
    Id INT,
    Age INT,
    Job STRING,
    Marital STRING,
    Education STRING,
    `Default` STRING,
    Balance FLOAT,
    HHInsurance STRING,
    CarLoan STRING,
    Communication STRING,
    LastContactDay INT,
    LastContactMonth STRING,
    NoOfContacts INT,
    DaysPassed INT,
    PrevAttempts INT,
    Outcome STRING,
    CallStart STRING,
    CallEnd STRING,
    CarInsurance STRING
)
clustered by(Age) into 4 buckets
row format delimited
fields terminated by ','
stored as textfile;


-- 3.	Add an additional partition on 'Job' to the partitioned table created earlier and move the data accordingly.

-- Hive does not support partition alteration. If we need new partition then we need to create a new table with required partition/s.

create external table if not exists car_insurance_calls_partitioned_new (
    Id INT,
    Age INT,
    `Default` STRING,
    Balance FLOAT,
    HHInsurance STRING,
    CarLoan STRING,
    Communication STRING,
    LastContactDay INT,
    LastContactMonth STRING,
    NoOfContacts INT,
    DaysPassed INT,
    PrevAttempts INT,
    Outcome STRING,
    CallStart STRING,
    CallEnd STRING,
    CarInsurance STRING
)
partitioned by (Education string, Marital string, Job STRING)
row format delimited
fields terminated by ','
stored as textfile;

-- loading data from car_insurance_calls_partitioned table into car_insurance_calls_partitioned_new 
insert overwrite table
car_insurance_calls_partitioned_new
partition(Education, Marital, Job)
select Id, Age, `Default`, Balance, HHInsurance,
CarLoan, Communication, LastContactDay,
LastContactMonth, NoOfContacts, DaysPassed,
PrevAttempts, Outcome, CallStart, CallEnd,
CarInsurance, Education, Marital, Job
from car_insurance_calls_partitioned;

-- 4.	Increase the number of buckets in the bucketed table to 10 and redistribute the data.
-- Hive does not support bucketing alteration on existing table if we need new bucket then we need to create new table with required buckets.


create table if not exists car_insurance_calls_buckted_new (
    Id INT,
    Age INT,
    Job STRING,
    Marital STRING,
    Education STRING,
    `Default` STRING,
    Balance FLOAT,
    HHInsurance STRING,
    CarLoan STRING,
    Communication STRING,
    LastContactDay INT,
    LastContactMonth STRING,
    NoOfContacts INT,
    DaysPassed INT,
    PrevAttempts INT,
    Outcome STRING,
    CallStart STRING,
    CallEnd STRING,
    CarInsurance STRING
)
clustered by(Age) into 10 buckets
row format delimited
fields terminated by ','
stored as textfile;

-- loading data into

insert overwrite table
car_insurance_calls_buckted_new
select * from car_insurance_calls_buckted;


-- Optimized Joins
-- 1.	Join the original table with the partitioned table and find out the average 'Balance' for each 'Job' and 'Education' level.

select o.Job, p.Education, avg(o.Balance) as
average_balance
from car_insurance_call o
join car_insurance_data_partitioned_new p on
o.Id = p.Id
group by o.Job, p.Education;


-- 2.	Join the original table with the bucketed table and calculate the total 'NoOfContacts' for each 'Age' group.

select o.Age, p.Education, sum(o.NoOfContacts) as
total_contacts
from car_insurance_call o
join car_insurance_calls_buckted_new b on
o.Id = b.Id
group by o.NoOfContacts, p.Age;


-- 3.	Join the partitioned table and the bucketed table based on the 'Id' field and find the total balance for each education level and marital status for each age group.

select p.Age, b.Education, p.Marital, sum(b.Balance) as
total_balance
from car_insurance_data_partitioned_new p
join car_insurance_calls_buckted_new b on
p.Id = b.Id
group by p.Age, p.Education, p.Marital;


-- Window Function
-- 1.	Calculate the cumulative sum of 'NoOfContacts' for each 'Job' category, ordered by 'Age'.

select Age, Job, NoOfContacts,
sum(NoOfContacts)
over (partition by Job order by Age) as cumulative_sum
from car_insurance_call
order by Age, Job;

-- 2.	Calculate the running average of 'Balance' for each 'Job' category, ordered by 'Age'.

select Age, Job, Balance,
sum(Balance)
over (partition by Job order by Age) as running_average
from car_insurance_call
order by Age, Job;

-- 3.	For each 'Job' category, find the maximum 'Balance' for each 'Age' group using window functions.

select Age, Job, Balance
from (
select Age, Job, Balance,
row_number() over (partition by
Job, Age order by Balance desc) as new
from car_insurance_call
) t
where new = 1
order by Job, Age;

-- 4.	Calculate the rank of 'Balance' within each 'Job' category, ordered by 'Balance' descending.

select Age, Job, Balance, rank() over
(partition by Job order by Balance
desc) as balance_rank
from car_insurance_call
order by Job, Balance desc;


-- Advanced Aggregations
-- 1.	Find the job category with the highest number of car insurances.

select Job
from (
select Job, COUNT(*) AS
car_insurance_count
from car_insurance_data
where CarInsurance = 1
group by Job
) t
order by car_insurance_count desc
limit 1;

-- 2.	Which month has seen the highest number of last contacts?

select LastContactMonth, COUNT(*) as
contact_count
from car_insurance_call
group by LastContactMonth
order by contact_count desc
limit 1;

-- 3.	Calculate the ratio of the number of customers with car insurance to the number of customers without car insurance for each job category.

select r1.Job, r1.car_insurance_count /
r2.no_car_insurance_count as
car_insurance_ratio
from (
select Job, COUNT(*) as
car_insurance_count
from car_insurance_call
where CarInsurance = 1
group by Job
) r1
join (
select Job, COUNT(*) as
no_car_insurance_count
from car_insurance_call
where CarInsurance = 0
group by Job
) r2
on r1.Job = r2.Job;

-- 4.	Find out the 'Job' and 'Education' level combination which has the highest number of car insurances.

select Job, Education
from (
select Job, Education, COUNT(*)
as car_insurance_count
from car_insurance_call
where CarInsurance = 1
group by Job, Education
) a
order by car_insurance_count
desc
limit 1;

-- 5.	Calculate the average 'NoOfContacts' for each 'Outcome' and 'Job' combination.

select Outcome, Job,
avg(NoOfContacts) AS
average_contacts
from car_insurance_call
group by Outcome, Job;

-- 6.	Determine the month with the highest total 'Balance' of customers.

select LastContactMonth,
sum(Balance) AS total_balance
from car_insurance_call
group by LastContactMonth
order by total_balance desc
limit 1;

-- Complex joins and aggregations
-- 1.	For customers who have both a car loan and home insurance, find out the average 'Balance' for each 'Education' level.

select Education, avg(Balance) as
average_balance
from (
select Education, Balance
from car_insurance_call
where CarLoan = 1 and
HHInsurance = 1
) t
group by Education;

-- 2.	Identify the top 3 'Communication' types for customers with 'CarInsurance', and display their average 'NoOfContacts'.

select Communication,
avg(NoOfContacts) as
average_contacts
from (
select Communication,
NoOfContacts
from car_insurance_call
where CarInsurance = 1
) t
group by Communication
order by average_contacts desc
limit 3;

-- 3.	For customers who have a car loan, calculate the average balance for each job category.

select Job, avg(Balance) as
average_balance
from car_insurance_call
where CarLoan = 1
group by Job;

-- 4.	Identify the top 5 job categories that have the most customers with a 'default', and show their average 'balance'.

select Job, AVG(Balance) as
average_balance
from (
select Job, Balance
from car_insurance_call
where Default = 1
) t
group by Job
order by count(*) desc
limit 5;

