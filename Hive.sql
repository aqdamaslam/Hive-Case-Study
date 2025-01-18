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
PARTITIONED BY (Education string, Marital string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table
car_insurance_calls_partitioned PARTITION(Education,
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

create table if not exists car_insurance_calls (
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


-- 

