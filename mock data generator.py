import pandas as pd
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Define the number of records to generate
num_records = 10000

# Define possible values for categorical fields
jobs = ['admin', 'technician', 'management', 'retired', 'blue-collar', 'entrepreneur', 'unknown', 'services', 'self-employed']
maritals = ['married', 'single', 'divorced', 'unknown']
educations = ['primary', 'secondary', 'tertiary', 'unknown']
defaults = ['yes', 'no']
communications = ['cellular', 'telephone', 'unknown']
outcomes = ['success', 'failure', 'unknown', 'other']
car_insurance_options = ['yes', 'no']
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

# Generate mock data
data = {
    'Id': [i for i in range(1, num_records + 1)],
    'Age': [random.randint(18, 70) for _ in range(num_records)],
    'Job': [random.choice(jobs) for _ in range(num_records)],
    'Marital': [random.choice(maritals) for _ in range(num_records)],
    'Education': [random.choice(educations) for _ in range(num_records)],
    '`Default`': [random.choice(defaults) for _ in range(num_records)],
    'Balance': [round(random.uniform(-5000, 50000), 2) for _ in range(num_records)],
    'HHInsurance': [random.choice(defaults) for _ in range(num_records)],
    'CarLoan': [random.choice(defaults) for _ in range(num_records)],
    'Communication': [random.choice(communications) for _ in range(num_records)],
    'LastContactDay': [random.randint(1, 31) for _ in range(num_records)],
    'LastContactMonth': [random.choice(months) for _ in range(num_records)],
    'NoOfContacts': [random.randint(0, 50) for _ in range(num_records)],
    'DaysPassed': [random.randint(-1, 500) for _ in range(num_records)],
    'PrevAttempts': [random.randint(0, 20) for _ in range(num_records)],
    'Outcome': [random.choice(outcomes) for _ in range(num_records)],
    'CallStart': [fake.time() for _ in range(num_records)],
    'CallEnd': [fake.time() for _ in range(num_records)],
    'CarInsurance': [random.choice(car_insurance_options) for _ in range(num_records)]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save DataFrame to CSV
df.to_csv('mock_data.csv', index=False)

print("Mock data generated and saved to 'mock_data.csv'.")
