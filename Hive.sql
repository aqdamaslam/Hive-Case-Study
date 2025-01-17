-- 1.	Load the data into a Hive table. Create an external table with the given schema and load the data into the table from a text file or HDFS path.

CREATE EXTERNAL TABLE IF NOT EXISTS car_insurance_calls (
    Id INT,
    Age INT,
    Job STRING,
    Marital STRING,
    Education STRING,
    Default STRING,
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
