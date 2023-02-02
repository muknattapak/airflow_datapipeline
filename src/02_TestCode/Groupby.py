import pandas as pd
# Import data
url = "/Users/thawaree/Documents/GitHub/zg-analytics-engine-python/Dataset test/Department_Dataset.csv"
employee = pd.read_csv(url)

url2 = "/Users/thawaree/Documents/GitHub/zg-analytics-engine-python/Dataset test/employee_data.csv"
employee2 = pd.read_csv(url2)
employee2 = employee2.drop(
    employee2.columns[[0]], axis=1)  # drop column by index

# Grouping and perform count over each group
dept_emp_num = employee.groupby('Dept_name')['Dept_name'].count()
print(dept_emp_num)

# Group by two keys and then summarize each group
dept_gender_salary = employee.groupby(
    ['Dept_name', 'location'], as_index=False).travel_required.count()
print(dept_gender_salary)

employee2.groupby(['groups', 'healthy_eating'], as_index=False).agg(
    {'healthy_eating': 'count', 'salary': 'mean'})
