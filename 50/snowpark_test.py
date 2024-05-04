from snowflake.snowpark import Session
Session = Session.builder.builder.config('local_testing').create()

# creating sample data
data = [
    ('DIVYANSH', 'KPMG', 26),
    ('PIYUSH', 'INFY', 37),
    ('ROHAN', 'TCS', 44),
    ('LEO', 'EY', 29),
    ('SACHIN', 'IBM', 34),
    ('MOHIT', 'IBM', 24),
    ('KARAN', 'KPMG', 54),
]

schema = ['Name', 'Org', 'Age']

df = Session.createDataFrame(data, schema)
df.show()
