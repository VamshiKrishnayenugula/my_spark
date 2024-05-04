from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# Create a SparkSession
spark = SparkSession.builder.appName('example1').getOrCreate()

# Start time
start_time = datetime.now()

# Sample data
data = [("James", "Smith", "M", 30),
        ("Anna", "Rose", "F", 41),
        ("Robert", "Williams", "M", 62)]

# Columns for the DataFrame
columns = ["FirstName", "LastName", "Gender", "Age"]

# Create a DataFrame with the given data and schema
df = spark.createDataFrame(data=data, schema=columns)

# Cache the DataFrame
#df.cache()

# Show the DataFrame
df.show()

# Filter the DataFrame to only show rows where age is greater than 50
filtered_df = df.filter(df["Age"] > 50)

# Show the filtered DataFrame
print("\nFiltered DataFrame:")
filtered_df.show()

# Group and aggregate the DataFrame to find the average age by gender
aggregated_df = df.groupBy('Gender').agg(F.avg('Age').alias('Average_age'))

# Show the grouped and aggregated DataFrame
print('\nGrouped and aggregated DataFrame:')
aggregated_df.show()

# End time
end_time = datetime.now()

# Calculate total time taken
total_time = end_time - start_time

# Print start, end and total time
print(f"Start time: {start_time}")
print(f"End time: {end_time}")
print(f"Total time taken: {total_time}")

# Stop the SparkSession
spark.stop()
