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
LOCATION '/user/aslam/bootcamp/hive/assignment-1/';


-- 1.	How many records are there in the dataset?

select count(*) from car_insurance_calls;


