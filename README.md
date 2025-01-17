**Case Study - Car Insurance Cold Calls Data Analysis**

### **Introduction**
This case study provides a comprehensive analysis of the "Car Insurance Cold Calls Data" using Apache Hive. The assignment covers various stages, including data loading, exploration, aggregation, partitioning, bucketing, optimized joins, window functions, advanced aggregations, complex joins, and performance tuning.

### **Data Loading**
1. **Create an External Table:**
   - Create an external Hive table with the given schema.
   - Load the data into the table from a text file or HDFS path.

### **Data Exploration**
1. **Total Records:**
   - Query to count the number of records in the dataset.
2. **Unique Job Categories:**
   - Query to determine the number of unique job categories.
3. **Age Distribution:**
   - Query to classify customers into age groups: 18-30, 31-45, 46-60, 61+.
4. **Missing Values:**
   - Query to count records with missing values in any field.
5. **Outcome Analysis:**
   - Query to find unique 'Outcome' values and their counts.
6. **Car Loan and Home Insurance:**
   - Query to find the number of customers with both a car loan and home insurance.

### **Aggregations**
1. **Balance Statistics by Job:**
   - Query to calculate the average, minimum, and maximum balance for each job category.
2. **Car Insurance Analysis:**
   - Query to count customers with and without car insurance.
3. **Communication Type Distribution:**
   - Query to count customers for each communication type.
4. **Balance by Communication Type:**
   - Query to calculate the sum of 'Balance' for each 'Communication' type.
5. **PrevAttempts by Outcome:**
   - Query to count 'PrevAttempts' for each 'Outcome' type.
6. **Contacts by Car Insurance:**
   - Query to calculate the average 'NoOfContacts' for customers with and without 'CarInsurance'.

### **Partitioning and Bucketing**
1. **Partitioned Table Creation:**
   - Create a partitioned table on 'Education' and 'Marital' status and load data.
2. **Bucketed Table Creation:**
   - Create a bucketed table on 'Age' with 4 groups and load data.
3. **Additional Partition on Job:**
   - Add an additional partition on 'Job' and redistribute the data.
4. **Increase Buckets:**
   - Increase the number of buckets in the bucketed table to 10 and redistribute the data.

### **Optimized Joins**
1. **Average Balance by Job and Education:**
   - Join the original table with the partitioned table to find the average balance.
2. **Contacts by Age Group:**
   - Join the original table with the bucketed table to calculate total 'NoOfContacts'.
3. **Balance by Education and Marital Status:**
   - Join the partitioned and bucketed tables based on 'Id' to find total balance.

### **Window Function**
1. **Cumulative Sum of Contacts:**
   - Calculate the cumulative sum of 'NoOfContacts' for each 'Job' category, ordered by 'Age'.
2. **Running Average of Balance:**
   - Calculate the running average of 'Balance' for each 'Job' category, ordered by 'Age'.
3. **Max Balance by Age Group:**
   - Find the maximum 'Balance' for each 'Age' group within each 'Job' category.
4. **Rank Balance by Job:**
   - Calculate the rank of 'Balance' within each 'Job' category, ordered by descending 'Balance'.

### **Advanced Aggregations**
1. **Top Job Category for Car Insurance:**
   - Identify the job category with the highest number of car insurances.
2. **Highest Contact Month:**
   - Determine which month saw the highest number of last contacts.
3. **Car Insurance Ratio by Job:**
   - Calculate the ratio of customers with car insurance to those without for each job category.
4. **Job and Education Combination:**
   - Find the 'Job' and 'Education' level combination with the highest number of car insurances.
5. **Average Contacts by Outcome and Job:**
   - Calculate the average 'NoOfContacts' for each 'Outcome' and 'Job' combination.
6. **Highest Balance Month:**
   - Identify the month with the highest total 'Balance' of customers.

### **Complex Joins and Aggregations**
1. **Average Balance for Loans and Insurance:**
   - For customers with both a car loan and home insurance, calculate the average 'Balance' for each 'Education' level.
2. **Top Communication Types:**
   - Identify the top 3 'Communication' types for customers with 'CarInsurance' and their average 'NoOfContacts'.
3. **Average Balance by Job for Loans:**
   - For customers with a car loan, calculate the average balance for each job category.
4. **Top Jobs with Default:**
   - Identify the top 5 job categories with the most customers with a 'default' and their average 'balance'.

### **Advanced Window Functions**
1. **Contact Difference by Job:**
   - Calculate the difference in 'NoOfContacts' between each customer and the next highest in the same 'Job' category.
2. **Balance Difference by Job:**
   - Calculate the difference between each customer's 'Balance' and the average 'Balance' of their 'Job' category.
3. **Longest Call Duration by Job:**
   - Find the customer with the longest call duration for each 'Job' category.
4. **Moving Average of Contacts:**
   - Calculate the moving average of 'NoOfContacts' within each 'Job' category, using a window frame of the current row and the two preceding rows.

### **Performance Tuning**
1. **File Format Experimentation:**
   - Experiment with ORC and Parquet formats and measure their impact on query performance.
2. **Compression Levels:**
   - Use different compression levels and observe their effects on storage and query performance.
3. **Bucketing Impact:**
   - Compare execution time of join queries with and without bucketing.
4. **Query Optimization Techniques:**
   - Optimize Hive queries using techniques like predicate pushdown and map-side joins, and discuss performance improvements.

