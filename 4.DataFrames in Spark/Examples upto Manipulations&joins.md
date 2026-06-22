This is an excellent master curriculum, but it's too large to generate in one response. The best approach is to generate **one module at a time**.

# Spark DataFrames Master Course

# PART 1: DATASET INTRODUCTION

We'll use a single **Healthcare Analytics Dataset** throughout the course.

---

# Business Scenario

A healthcare company operates multiple hospitals across India.

Management wants to analyze:

* Patient Admissions
* Treatment Costs
* Insurance Coverage
* Hospital Performance
* Patient Satisfaction
* Department Performance
* Revenue Analytics

This dataset will be used throughout all Spark DataFrame concepts.

---

# Dataset Schema

| Column Name                | Data Type |
| -------------------------- | --------- |
| Patient_ID                 | Integer   |
| Patient_Name               | String    |
| Age                        | Integer   |
| Gender                     | String    |
| Hospital_ID                | Integer   |
| Hospital_Name              | String    |
| Department                 | String    |
| Doctor_Name                | String    |
| Admission_Date             | Date      |
| Discharge_Date             | Date      |
| Diagnosis                  | String    |
| Treatment_Cost             | Double    |
| Insurance_Amount           | Double    |
| City                       | String    |
| State                      | String    |
| Severity_Level             | String    |
| Visitors_Count             | Integer   |
| Patient_Satisfaction_Score | Integer   |

---

# Sample Records

| Patient_ID | Patient_Name | Age | Gender | Hospital_Name    | Department  | Treatment_Cost | Insurance_Amount |
| ---------- | ------------ | --- | ------ | ---------------- | ----------- | -------------- | ---------------- |
| 101        | Ravi Kumar   | 45  | M      | Apollo Hyderabad | Cardiology  | 120000         | 80000            |
| 102        | Priya Sharma | 32  | F      | Apollo Hyderabad | Neurology   | 85000          | 50000            |
| 103        | Kiran Reddy  | 55  | M      | Yashoda          | Orthopedics | 150000         | 100000           |
| 104        | Sneha Rao    | 28  | F      | Care Hospital    | ENT         | 45000          | 30000            |
| 105        | Arjun Patel  | 60  | M      | Apollo Hyderabad | Cardiology  | 200000         | 150000           |

---

# Business Meaning of Each Column

## Patient_ID

Unique patient identifier.

Example:

```text
101
102
103
```

---

## Patient_Name

Patient's full name.

```text
Ravi Kumar
Priya Sharma
```

---

## Age

Patient age.

```text
45
32
55
```

Useful for:

* Age-group analysis
* Risk prediction
* Healthcare segmentation

---

## Gender

```text
M
F
```

Used for demographic analytics.

---

## Hospital_ID

Unique hospital identifier.

```text
1
2
3
```

---

## Hospital_Name

Hospital where patient received treatment.

```text
Apollo Hyderabad
Yashoda
Care Hospital
```

---

## Department

Treatment department.

```text
Cardiology
Neurology
Orthopedics
ENT
```

---

## Doctor_Name

Attending doctor.

---

## Admission_Date

Patient admission date.

```text
2025-01-05
```

---

## Discharge_Date

Patient discharge date.

Used for:

```text
Length Of Stay (LOS)
```

---

## Diagnosis

Disease diagnosed.

```text
Heart Disease
Migraine
Fracture
```

---

## Treatment_Cost

Total treatment expense.

```text
120000
85000
```

---

## Insurance_Amount

Insurance-covered amount.

```text
80000
50000
```

---

## City

Patient city.

---

## State

Patient state.

---

## Severity_Level

```text
Low
Medium
High
Critical
```

---

## Visitors_Count

Number of visitors.

---

## Patient_Satisfaction_Score

Rating:

```text
1 to 10
```

Used for hospital quality analysis.

---

# MODULE 1 — DATAFRAME CREATION

---

# Why DataFrames?

RDDs have limitations:

❌ No schema

❌ Hard to optimize

❌ Difficult syntax

DataFrames provide:

✅ Structured data

✅ Schema

✅ Catalyst Optimizer

✅ Faster execution

✅ SQL-like operations

---

# Visual Representation

RDD

```text
["101 Ravi 45"]
["102 Priya 32"]
["103 Kiran 55"]
```

DataFrame

```text
+----+------+---+
|ID  |Name  |Age|
+----+------+---+
|101 |Ravi  |45 |
|102 |Priya |32 |
|103 |Kiran |55 |
+----+------+---+
```

---

# 1. Create DataFrame From List

## Syntax

```python
spark.createDataFrame(data, schema)
```

## Example

```python
data = [
    (101,"Ravi",45),
    (102,"Priya",32),
    (103,"Kiran",55)
]

columns = ["Patient_ID","Patient_Name","Age"]

df = spark.createDataFrame(data,columns)

df.show()
```

Output

```text
+----------+------------+---+
|Patient_ID|Patient_Name|Age|
+----------+------------+---+
|101       |Ravi        |45 |
|102       |Priya       |32 |
|103       |Kiran       |55 |
+----------+------------+---+
```

---

# 2. Create DataFrame From RDD

## Step 1

Create RDD

```python
rdd = spark.sparkContext.parallelize([
    (101,"Ravi",45),
    (102,"Priya",32),
    (103,"Kiran",55)
])
```

## Step 2

Convert RDD → DataFrame

```python
df = rdd.toDF([
    "Patient_ID",
    "Patient_Name",
    "Age"
])

df.show()
```

---

# 3. Create DataFrame From CSV

## Syntax

```python
df = spark.read.csv(
    "patients.csv",
    header=True,
    inferSchema=True
)
```

---

# 4. Explicit Schema

Recommended in Production.

```python
from pyspark.sql.types import *
```

```python
schema = StructType([
    StructField("Patient_ID",IntegerType(),True),
    StructField("Patient_Name",StringType(),True),
    StructField("Age",IntegerType(),True)
])
```

```python
df = spark.read.csv(
    "patients.csv",
    header=True,
    schema=schema
)
```

---

# Why Explicit Schema?

Production Advantages:

✔ Faster

✔ Safer

✔ Consistent

✔ No incorrect datatype inference

---

# 5. Infer Schema

```python
df = spark.read.csv(
    "patients.csv",
    header=True,
    inferSchema=True
)
```

Spark automatically determines:

```text
Age → Integer
Cost → Double
Name → String
```

---

# 6. Rename Columns

Method 1

```python
df = df.withColumnRenamed(
    "Patient_Name",
    "Name"
)
```

Method 2

```python
df.toDF(
    "ID",
    "Name",
    "Age"
)
```

---

# 7. Select Columns

## Single Column

```python
df.select("Patient_Name").show()
```

## Multiple Columns

```python
df.select(
    "Patient_Name",
    "Age",
    "Department"
).show()
```

---

# Beginner Problems

## Problem 1

Create DataFrame from list containing:

```text
Patient_ID
Patient_Name
Age
```

### Solution

```python
data = [
(101,"Ravi",45),
(102,"Priya",32)
]

df = spark.createDataFrame(
    data,
    ["Patient_ID","Patient_Name","Age"]
)

df.show()
```

---

## Problem 2

Display only patient names.

### Solution

```python
df.select("Patient_Name").show()
```

---

## Problem 3

Rename Age → Patient_Age.

### Solution

```python
df.withColumnRenamed(
    "Age",
    "Patient_Age"
).show()
```

---

# Intermediate Problems

## Problem 1

Load CSV and infer schema.

```python
df = spark.read.csv(
    "patients.csv",
    header=True,
    inferSchema=True
)
```

---

## Problem 2

Select:

```text
Patient_Name
Department
Treatment_Cost
```

```python
df.select(
"Patient_Name",
"Department",
"Treatment_Cost"
).show()
```

---

## Problem 3

Convert RDD to DataFrame.

```python
rdd = spark.sparkContext.parallelize(data)

df = rdd.toDF(columns)
```

---

# Advanced Problems

## Problem 1

Create DataFrame using explicit schema.

(Production-level answer shown above.)

---

## Problem 2

Rename all columns.

```python
df = df.toDF(
"ID",
"Name",
"Age",
"Gender"
)
```

---

## Problem 3

Read large CSV with optimized schema.

```python
df = spark.read \
    .option("header",True) \
    .schema(schema) \
    .csv("patients.csv")
```

---

# Interview Questions

### Q1. Why DataFrame over RDD?

**Answer:**

* Schema support
* Catalyst optimization
* Better performance
* Easier syntax

---

### Q2. Difference between inferSchema and explicit schema?

**Answer:**

inferSchema:

```text
Automatic
Slower
```

Explicit Schema:

```text
Manual
Faster
Production Preferred
```

---

### Q3. How do you create DataFrame from RDD?

```python
rdd.toDF(columns)
```

---

### Q4. Why is SparkSession preferred?

Because SparkSession includes:

```text
SparkContext
SQLContext
HiveContext
```

---

### Q5. Which schema approach is recommended in production?

✅ Explicit Schema

❌ inferSchema

---

# Common Mistakes

❌ Using inferSchema on huge datasets

❌ Reading everything as String

❌ Renaming columns repeatedly

❌ Using RDD when DataFrame is sufficient

---

# Best Practices

✅ Use SparkSession

✅ Use Explicit Schema

✅ Select only required columns

✅ Avoid inferSchema in production

✅ Keep column names meaningful

---

 **Module 2: DataFrame Inspection** in the same detailed textbook style.

# MODULE 2: DATAFRAME INSPECTION

Before performing filtering, joins, aggregations, or machine learning, a Data Engineer/Data Analyst must first **understand the dataset**.

This process is called:

# Data Inspection (Data Exploration)

---

# Why Data Inspection is Important?

Imagine receiving a dataset with:

```text
50 Million Rows
200 Columns
```

Would you directly start writing queries?

❌ No

First you inspect:

* What columns exist?
* What are their datatypes?
* How many records?
* Any missing values?
* Any strange values?
* Summary statistics?

This is exactly what DataFrame Inspection helps us do.

---

# Visual Workflow

```text
Raw Dataset
     │
     ▼
Data Inspection
     │
     ├── printSchema()
     │
     ├── columns
     │
     ├── show()
     │
     ├── count()
     │
     ├── describe()
     │
     └── summary()
     │
     ▼
Data Cleaning
     ▼
Analysis
```

---

# Healthcare Dataset

We'll continue using:

```text
Patient_ID
Patient_Name
Age
Gender
Hospital_ID
Hospital_Name
Department
Doctor_Name
Admission_Date
Discharge_Date
Diagnosis
Treatment_Cost
Insurance_Amount
City
State
Severity_Level
Visitors_Count
Patient_Satisfaction_Score
```

---

# 1. printSchema()

---

# What is printSchema()?

Displays:

* Column Names
* Data Types
* Nullable Information

---

# Why is it Important?

Suppose:

```text
Age = String
```

instead of

```text
Age = Integer
```

Then:

```python
avg(Age)
```

will fail.

Therefore checking schema is the first step.

---

# Syntax

```python
df.printSchema()
```

---

# Example

```python
df.printSchema()
```

Output

```text
root
 |-- Patient_ID: integer
 |-- Patient_Name: string
 |-- Age: integer
 |-- Gender: string
 |-- Treatment_Cost: double
 |-- Insurance_Amount: double
```

---

# Visual Explanation

```text
DataFrame

Patient_ID
Patient_Name
Age
Gender

        │
        ▼

printSchema()

Patient_ID  → Integer
Patient_Name → String
Age → Integer
Gender → String
```

---

# Beginner Problem 1

Display schema of healthcare dataset.

### Solution

```python
df.printSchema()
```

---

# Beginner Problem 2

Identify datatype of Treatment_Cost.

### Solution

```python
df.printSchema()
```

Look for:

```text
Treatment_Cost : double
```

---

# Beginner Problem 3

Check if Age is Integer.

### Solution

```python
df.printSchema()
```

---

# Interview Question

### Difference between schema and data?

Schema:

```text
Structure
```

Data:

```text
Actual records
```

---

# 2. columns

---

# What is columns?

Returns list of all column names.

---

# Syntax

```python
df.columns
```

---

# Example

```python
df.columns
```

Output

```python
[
'Patient_ID',
'Patient_Name',
'Age',
'Gender',
'Hospital_Name',
'Department'
]
```

---

# Why Used?

Real projects often contain:

```text
200+
Columns
```

Need to know available columns quickly.

---

# Visual

```text
DataFrame
   │
   ▼

columns

[
Patient_ID,
Patient_Name,
Age,
Gender
]
```

---

# Beginner Problem

Get all column names.

### Solution

```python
df.columns
```

---

# Intermediate Problem

Count total columns.

### Solution

```python
len(df.columns)
```

---

# Advanced Problem

Check whether column exists.

```python
'Department' in df.columns
```

Output

```python
True
```

---

# Interview Question

### Does columns return DataFrame?

No

Returns:

```python
list
```

---

# 3. show()

---

# What is show()?

Displays records.

---

# Syntax

```python
df.show()
```

---

# Example

```python
df.show()
```

Output

```text
+----------+------------+---+
|Patient_ID|Patient_Name|Age|
+----------+------------+---+
|101       |Ravi        |45 |
|102       |Priya       |32 |
|103       |Kiran       |55 |
+----------+------------+---+
```

---

# Show Top N Rows

```python
df.show(5)
```

---

# Show Complete Data

```python
df.show(truncate=False)
```

---

# Visual

```text
show()

Displays

Top Rows
```

---

# Beginner Problems

### Show first 5 records

```python
df.show(5)
```

---

### Show first 10 records

```python
df.show(10)
```

---

### Show without truncation

```python
df.show(truncate=False)
```

---

# Interview Question

### Difference between show() and collect()?

show()

```text
Displays
```

collect()

```text
Returns data to Driver
```

---

# 4. count()

---

# What is count()?

Returns total number of rows.

---

# Syntax

```python
df.count()
```

---

# Example

```python
df.count()
```

Output

```text
20
```

---

# Why Important?

Business questions:

```text
How many patients?
How many transactions?
How many customers?
```

All answered using count().

---

# Visual

```text
20 Rows

count()

↓

20
```

---

# Beginner Problem

Find total patients.

```python
df.count()
```

---

# Intermediate Problem

Count records in Cardiology.

```python
df.filter(
    df.Department=="Cardiology"
).count()
```

---

# Advanced Problem

Count patients above age 50.

```python
df.filter(
    df.Age > 50
).count()
```

---

# Interview Question

### Is count expensive?

Yes.

Because Spark scans the entire dataset.

---

# 5. describe()

---

# What is describe()?

Provides basic statistics.

For numeric columns:

```text
count
mean
stddev
min
max
```

---

# Syntax

```python
df.describe().show()
```

---

# Example

```python
df.describe().show()
```

Output

```text
+-------+-----+-------+
|summary|Age  |Cost   |
+-------+-----+-------+
|count  |20   |20     |
|mean   |42.3 |95000  |
|stddev |10.5 |35000  |
|min    |18   |15000  |
|max    |75   |300000 |
+-------+-----+-------+
```

---

# Visual

```text
Age Column

45
32
55
28

↓

describe()

Count
Mean
Min
Max
StdDev
```

---

# Beginner Problem

Get Age statistics.

```python
df.describe(["Age"]).show()
```

---

# Intermediate Problem

Get Treatment Cost statistics.

```python
df.describe(
["Treatment_Cost"]
).show()
```

---

# Advanced Problem

Get Age and Cost statistics.

```python
df.describe(
["Age","Treatment_Cost"]
).show()
```

---

# Interview Question

### Does describe() work for strings?

Limited.

Mainly useful for numeric columns.

---

# 6. summary()

---

# What is summary()?

Enhanced version of describe().

Provides:

```text
count
mean
stddev
min
25%
50%
75%
max
```

---

# Syntax

```python
df.summary().show()
```

---

# Example

```python
df.summary().show()
```

Output

```text
+-------+-----+
|summary|Age  |
+-------+-----+
|count  |20   |
|mean   |42.3 |
|stddev |10.5 |
|min    |18   |
|25%    |30   |
|50%    |42   |
|75%    |55   |
|max    |75   |
+-------+-----+
```

---

# Why Important?

Used for:

* Outlier Detection
* Data Profiling
* Data Quality Analysis

---

# Visual

```text
Age Distribution

18
25
30
40
42
55
75

↓

summary()

25%
50%
75%
```

---

# Beginner Problem

Generate complete summary.

```python
df.summary().show()
```

---

# Intermediate Problem

Summary of Age only.

```python
df.summary("count",
           "mean",
           "min",
           "max").show()
```

---

# Advanced Problem

Summary of Age and Cost.

```python
df.select(
"Age",
"Treatment_Cost"
).summary().show()
```

---

# Real-World Healthcare Analysis

Suppose:

```python
df.summary().show()
```

Result:

```text
Average Age = 48

Average Treatment Cost = 1,25,000

Maximum Cost = 8,50,000
```

Business Insights:

✅ Most patients are middle-aged

✅ High treatment cost variation

✅ Need insurance optimization

---

# Practice Assignment

## Easy

1. Print schema.
2. Display column names.
3. Show first 5 rows.
4. Count rows.
5. Describe Age.

### Solutions

```python
df.printSchema()

df.columns

df.show(5)

df.count()

df.describe("Age").show()
```

---

## Medium

1. Count Cardiology patients.
2. Show first 10 records.
3. Count unique hospitals.
4. Describe Treatment_Cost.
5. Count patients above 60.

### Solutions

```python
df.filter(
df.Department=="Cardiology"
).count()

df.show(10)

df.select(
"Hospital_Name"
).distinct().count()

df.describe(
"Treatment_Cost"
).show()

df.filter(
df.Age>60
).count()
```

---

## Advanced

1. Summary of Age and Cost.
2. Count Critical patients.
3. Count patients per city.
4. Find maximum age.
5. Find minimum treatment cost.

### Solutions

```python
df.select(
"Age",
"Treatment_Cost"
).summary().show()

df.filter(
df.Severity_Level=="Critical"
).count()

df.groupBy("City").count().show()

df.agg(
{"Age":"max"}
).show()

df.agg(
{"Treatment_Cost":"min"}
).show()
```

---

# Interview Questions (Top 5)

### Q1. Difference between describe() and summary()?

**describe()**

```text
count
mean
stddev
min
max
```

**summary()**

```text
count
mean
stddev
min
25%
50%
75%
max
```

---

### Q2. Does count() trigger Spark execution?

✅ Yes

It is an Action.

---

### Q3. Does show() trigger Spark execution?

✅ Yes

It is an Action.

---

### Q4. What does printSchema() return?

Schema structure and datatypes.

---

### Q5. What does columns return?

Python list of column names.

---

# Common Mistakes

❌ Using count() repeatedly on huge datasets

❌ Not checking schema before analysis

❌ Using show() on very large outputs

❌ Assuming inferSchema is always correct

---

# Best Practices

✅ Always inspect schema first

✅ Use show(5) instead of show() frequently

✅ Use summary() for profiling

✅ Check column names before transformations

✅ Validate datatypes before aggregations

---

**Module 3: Column Operations (withColumn, cast, alias, drop, withColumnRenamed)** with the same deep level of explanation, business examples, interview questions, and practice problems.

# MODULE 3: COLUMN OPERATIONS

Column Operations are among the **most frequently used DataFrame operations in real-world Spark projects**.

Almost every ETL Pipeline, Data Engineering workflow, Data Cleaning process, and Machine Learning pipeline involves creating, modifying, renaming, or removing columns.

---

# Why Column Operations Matter

Suppose a hospital dataset arrives like this:

| Patient_ID | Age | Treatment_Cost |
| ---------- | --- | -------------- |
| 101        | 45  | 120000         |
| 102        | 32  | 85000          |

Business wants:

| Patient_ID | Age | Age_Group | Treatment_Cost | Net_Cost |
| ---------- | --- | --------- | -------------- | -------- |
| 101        | 45  | Adult     | 120000         | 40000    |
| 102        | 32  | Adult     | 85000          | 35000    |

To achieve this, we need:

✅ Create columns

✅ Rename columns

✅ Change datatypes

✅ Drop columns

✅ Use aliases

---

# Topics Covered

## 1. withColumn()

## 2. withColumnRenamed()

## 3. cast()

## 4. alias()

## 5. drop()

---

# Healthcare Dataset

We continue using:

```text
Patient_ID
Patient_Name
Age
Gender
Treatment_Cost
Insurance_Amount
Department
Hospital_Name
```

---

# 1. withColumn()

---

# What is withColumn()?

Used to:

* Create new column
* Modify existing column
* Replace existing column

---

# Syntax

```python
df.withColumn(
    "new_column",
    expression
)
```

---

# Visual Explanation

```text
Before

+----------+----+
|Patient_ID|Age |
+----------+----+
|101       |45  |
|102       |32  |
+----------+----+

          │
          ▼

withColumn("Age_Plus_5",Age+5)

          │
          ▼

+----------+----+----------+
|Patient_ID|Age |Age_Plus_5|
+----------+----+----------+
|101       |45  |50        |
|102       |32  |37        |
+----------+----+----------+
```

---

# Example 1

Create New Column

```python
from pyspark.sql.functions import col

df = df.withColumn(
    "Age_After_5_Years",
    col("Age") + 5
)

df.show()
```

Output

```text
+---+---+----------------+
|ID |Age|Age_After_5_Years|
+---+---+----------------+
|101|45 |50              |
|102|32 |37              |
+---+---+----------------+
```

---

# Example 2

Net Treatment Cost

Business wants:

```text
Net Cost =
Treatment Cost - Insurance Amount
```

```python
df = df.withColumn(
    "Net_Cost",
    col("Treatment_Cost")
    -
    col("Insurance_Amount")
)
```

---

# Real-World Use Cases

### Banking

```text
Loan_Balance
```

### Uber

```text
Trip_Duration
```

### Netflix

```text
Watch_Time_Hours
```

### Healthcare

```text
Length_Of_Stay
Net_Cost
Risk_Score
```

---

# Beginner Problem 1

Create:

```text
Age + 10
```

### Solution

```python
df.withColumn(
    "Age_Plus_10",
    col("Age")+10
).show()
```

---

# Beginner Problem 2

Create:

```text
Double_Cost
```

```python
df.withColumn(
    "Double_Cost",
    col("Treatment_Cost")*2
).show()
```

---

# Beginner Problem 3

Create:

```text
Insurance_Remaining
```

```python
df.withColumn(
    "Insurance_Remaining",
    col("Treatment_Cost")
    -
    col("Insurance_Amount")
).show()
```

---

# Interview Question

### Does withColumn create a new DataFrame?

✅ Yes

DataFrames are immutable.

---

# 2. withColumnRenamed()

---

# What is withColumnRenamed()?

Renames an existing column.

---

# Syntax

```python
df.withColumnRenamed(
    "old_name",
    "new_name"
)
```

---

# Example

Before

```text
Patient_Name
```

After

```text
Patient_Full_Name
```

Code

```python
df = df.withColumnRenamed(
    "Patient_Name",
    "Patient_Full_Name"
)
```

---

# Visual

```text
Patient_Name
      │
      ▼

withColumnRenamed()

      │
      ▼

Patient_Full_Name
```

---

# Real-World Usage

Often source systems provide:

```text
cust_nm
txn_amt
hsp_cd
```

We rename them:

```text
Customer_Name
Transaction_Amount
Hospital_Code
```

for readability.

---

# Beginner Problem

Rename:

```text
Age
```

to

```text
Patient_Age
```

### Solution

```python
df.withColumnRenamed(
    "Age",
    "Patient_Age"
)
```

---

# Intermediate Problem

Rename:

```text
Treatment_Cost
```

to

```text
Hospital_Bill
```

### Solution

```python
df.withColumnRenamed(
    "Treatment_Cost",
    "Hospital_Bill"
)
```

---

# Interview Question

### Can withColumnRenamed change datatype?

❌ No

Only renames column.

---

# 3. cast()

---

# What is cast()?

Used to convert datatype.

---

# Why Important?

Suppose:

```text
Age = String
```

but you need:

```text
Age = Integer
```

for:

```python
avg(Age)
```

---

# Syntax

```python
col("Age").cast("integer")
```

---

# Example

```python
df = df.withColumn(
    "Age",
    col("Age").cast("integer")
)
```

---

# Visual

```text
Before

Age = "45"

String

      │
      ▼

cast("integer")

      │
      ▼

Age = 45

Integer
```

---

# Example

Convert Cost to Double

```python
df = df.withColumn(
    "Treatment_Cost",
    col("Treatment_Cost")
    .cast("double")
)
```

---

# Real-World Use Cases

### CSV Data

Everything may arrive as:

```text
String
```

Need conversion to:

```text
Integer
Double
Date
Timestamp
```

---

# Beginner Problem

Convert Age to Integer.

### Solution

```python
df.withColumn(
    "Age",
    col("Age").cast("integer")
)
```

---

# Intermediate Problem

Convert Cost to Double.

```python
df.withColumn(
    "Treatment_Cost",
    col("Treatment_Cost")
    .cast("double")
)
```

---

# Advanced Problem

Convert Patient_ID to String.

```python
df.withColumn(
    "Patient_ID",
    col("Patient_ID")
    .cast("string")
)
```

---

# Interview Question

### What happens if cast fails?

Spark returns:

```text
NULL
```

---

# Example

```python
"ABC"
```

cast to Integer

Result:

```text
NULL
```

---

# 4. alias()

---

# What is alias()?

Used to give temporary names.

Mostly used in:

* select()
* aggregations
* joins

---

# Syntax

```python
col("Age").alias("Patient_Age")
```

---

# Example

```python
df.select(
    col("Age")
    .alias("Patient_Age")
).show()
```

Output

```text
+-----------+
|Patient_Age|
+-----------+
|45         |
|32         |
+-----------+
```

---

# Visual

```text
Age
 │
 ▼

alias("Patient_Age")

 │
 ▼

Patient_Age
```

---

# Aggregation Example

```python
from pyspark.sql.functions import avg

df.select(
    avg("Treatment_Cost")
    .alias("Average_Cost")
).show()
```

---

# Real-World Usage

Instead of:

```text
avg(Treatment_Cost)
```

Use:

```text
Average_Cost
```

More readable.

---

# Beginner Problem

Alias Age.

```python
df.select(
col("Age")
.alias("Patient_Age")
).show()
```

---

# Intermediate Problem

Alias Cost.

```python
df.select(
col("Treatment_Cost")
.alias("Cost")
)
```

---

# Advanced Problem

Alias Aggregated Cost.

```python
df.select(
avg("Treatment_Cost")
.alias("Avg_Cost")
)
```

---

# Interview Question

### Does alias permanently rename a column?

❌ No

Temporary only.

---

# 5. drop()

---

# What is drop()?

Removes unwanted columns.

---

# Syntax

```python
df.drop("column_name")
```

---

# Example

Drop Insurance Column

```python
df = df.drop(
    "Insurance_Amount"
)
```

---

# Visual

```text
Before

Patient_ID
Age
Insurance_Amount

         │
         ▼

drop()

         │
         ▼

Patient_ID
Age
```

---

# Multiple Columns

```python
df.drop(
    "Insurance_Amount",
    "Gender"
)
```

---

# Real-World Use Cases

Remove:

```text
PII Columns

Email
Phone
Address
```

before analytics.

---

# Beginner Problem

Drop Gender.

### Solution

```python
df.drop("Gender")
```

---

# Intermediate Problem

Drop Age and Gender.

```python
df.drop(
"Age",
"Gender"
)
```

---

# Advanced Problem

Drop multiple sensitive columns.

```python
df.drop(
"Phone",
"Address",
"Email"
)
```

---

# Practice Assignment

# Easy

### Q1

Create Age+5 column.

### Q2

Rename Age.

### Q3

Convert Age to Integer.

### Q4

Alias Cost.

### Q5

Drop Gender.

---

# Solutions

```python
df.withColumn(
"Age_Plus_5",
col("Age")+5
)

df.withColumnRenamed(
"Age",
"Patient_Age"
)

df.withColumn(
"Age",
col("Age")
.cast("integer")
)

df.select(
col("Treatment_Cost")
.alias("Cost")
)

df.drop("Gender")
```

---

# Medium

### Q1

Create Net_Cost.

### Q2

Rename Treatment_Cost.

### Q3

Convert Cost to Double.

### Q4

Alias Average Cost.

### Q5

Drop Age and Gender.

---

# Advanced

### Q1

Create Profit Column.

### Q2

Create Risk Score.

### Q3

Convert Date Strings to Date Type.

### Q4

Alias Multiple Aggregations.

### Q5

Remove Sensitive Columns.

---

# Interview Questions (Top 5)

### Q1

Difference between alias() and withColumnRenamed()?

**Answer**

| alias()        | withColumnRenamed() |
| -------------- | ------------------- |
| Temporary      | Permanent           |
| select() usage | DataFrame usage     |

---

### Q2

Can withColumn modify existing columns?

✅ Yes

---

### Q3

What happens when cast fails?

✅ NULL returned.

---

### Q4

Does drop modify original DataFrame?

❌ No

Returns new DataFrame.

---

### Q5

Which operation is most used in Feature Engineering?

✅ withColumn()

---

# Common Mistakes

❌ Forgetting DataFrames are immutable

❌ Using alias instead of rename

❌ Casting wrong datatype

❌ Dropping important columns

---

# Best Practices

✅ Use meaningful names

✅ Use explicit casts

✅ Use alias in aggregations

✅ Minimize unnecessary columns

✅ Keep schema clean

---

# MODULE 3 REVISION SHEET

```text
withColumn()
    Create/Modify Columns

withColumnRenamed()
    Rename Column

cast()
    Change Datatype

alias()
    Temporary Name

drop()
    Remove Columns
```


**Module 4:**

# MODULE 4: FILTERING RECORDS IN SPARK DATAFRAMES

---

# What is Filtering?

Filtering means selecting only those records that satisfy specific conditions.

Think of filtering as applying a **WHERE clause in SQL**.

Example:

Hospital has 1 million patient records.

Management asks:

> Show only Cardiology patients.

You don't need all records.

You filter the required records.

---

# Why Filtering is Important?

In real-world projects:

* Find High-Risk Patients
* Find Failed Transactions
* Find Premium Customers
* Find Fraudulent Activities
* Find Delayed Deliveries
* Find Expensive Treatments

Filtering is one of the most frequently used Spark operations.

---

# Visual Explanation

Original Data

```text
+----------+-----------+------------+
|Patient_ID|Department |Cost        |
+----------+-----------+------------+
|101       |Cardiology |120000      |
|102       |Neurology  |85000       |
|103       |Cardiology |150000      |
+----------+-----------+------------+
```

Filter:

```python
df.filter(df.Department=="Cardiology")
```

Result

```text
+----------+-----------+------------+
|101       |Cardiology |120000      |
|103       |Cardiology |150000      |
+----------+-----------+------------+
```

---

# Topics Covered

## 1. where()

## 2. filter()

## 3. Single Conditions

## 4. Multiple Conditions

## 5. AND

## 6. OR

## 7. IN

## 8. NOT IN

## 9. BETWEEN

---

# 1. where()

---

# What is where()?

Filters records using a condition.

Equivalent to SQL WHERE.

---

# Syntax

```python
df.where(condition)
```

---

# Example

Retrieve Cardiology Patients

```python
df.where(
    df.Department=="Cardiology"
).show()
```

Output

```text
+----------+-----------+
|Patient_ID|Department |
+----------+-----------+
|101       |Cardiology |
|103       |Cardiology |
+----------+-----------+
```

---

# SQL Equivalent

```sql
SELECT *
FROM patients
WHERE Department='Cardiology';
```

---

# 2. filter()

---

# What is filter()?

Exactly same as where().

Both perform identical operations.

---

# Syntax

```python
df.filter(condition)
```

---

# Example

```python
df.filter(
    df.Department=="Cardiology"
).show()
```

Output is same as where().

---

# Interview Question

### Difference between where() and filter()?

Answer:

```text
No Difference
```

Both perform same operation.

---

# Single Condition Filtering

---

# Example 1

Age > 50

```python
df.filter(
    df.Age > 50
).show()
```

---

# Example 2

Treatment Cost > 100000

```python
df.filter(
    df.Treatment_Cost > 100000
).show()
```

---

# Example 3

Severity Level = Critical

```python
df.filter(
    df.Severity_Level=="Critical"
).show()
```

---

# Visual

```text
All Patients

      │
      ▼

Age > 50

      │
      ▼

Only Older Patients
```

---

# Beginner Problems

### Problem 1

Find patients older than 40.

### Solution

```python
df.filter(
df.Age > 40
).show()
```

---

### Problem 2

Find patients with cost above 50000.

```python
df.filter(
df.Treatment_Cost > 50000
).show()
```

---

### Problem 3

Find Cardiology patients.

```python
df.filter(
df.Department=="Cardiology"
).show()
```

---

# Multiple Conditions

Real business problems rarely use a single condition.

Usually:

```text
Age > 50
AND
Cost > 100000
```

or

```text
Cardiology
AND
Critical
```

---

# AND Condition

---

# Syntax

```python
(condition1) & (condition2)
```

---

# Example

Find:

```text
Age > 50

AND

Treatment Cost > 100000
```

```python
df.filter(
    (df.Age > 50)
    &
    (df.Treatment_Cost > 100000)
).show()
```

---

# Visual

```text
Age > 50

      AND

Cost > 100000

      │
      ▼

Only records satisfying BOTH
```

---

# Real Healthcare Example

Find Critical Cardiology Patients.

```python
df.filter(
    (df.Department=="Cardiology")
    &
    (df.Severity_Level=="Critical")
).show()
```

---

# Beginner Problem

Find:

```text
Age > 40
AND
Gender = M
```

Solution

```python
df.filter(
(df.Age>40)
&
(df.Gender=="M")
)
```

---

# OR Condition

---

# Syntax

```python
(condition1) | (condition2)
```

---

# Example

Retrieve:

```text
Cardiology

OR

Neurology
```

```python
df.filter(
    (df.Department=="Cardiology")
    |
    (df.Department=="Neurology")
).show()
```

---

# Visual

```text
Cardiology

OR

Neurology

      │
      ▼

Any matching condition
```

---

# Real Healthcare Example

Critical OR High Severity

```python
df.filter(
(df.Severity_Level=="Critical")
|
(df.Severity_Level=="High")
)
```

---

# Intermediate Problem

Find:

```text
Age > 60

OR

Cost > 200000
```

Solution

```python
df.filter(
(df.Age>60)
|
(df.Treatment_Cost>200000)
)
```

---

# IN Operator

---

# What is IN?

Equivalent of SQL IN.

Used when checking multiple values.

---

# Syntax

```python
col.isin()
```

---

# Example

Retrieve:

```text
Cardiology
Neurology
Orthopedics
```

```python
df.filter(
df.Department.isin(
"Cardiology",
"Neurology",
"Orthopedics"
)
).show()
```

---

# SQL Equivalent

```sql
WHERE Department IN
(
'Cardiology',
'Neurology',
'Orthopedics'
)
```

---

# Visual

```text
Department

Cardiology
Neurology
Orthopedics

      │
      ▼

Selected
```

---

# Intermediate Problem

Find patients from:

```text
Hyderabad
Mumbai
Bangalore
```

Solution

```python
df.filter(
df.City.isin(
"Hyderabad",
"Mumbai",
"Bangalore"
)
)
```

---

# NOT IN Operator

---

# What is NOT IN?

Opposite of IN.

---

# Syntax

```python
~col.isin(...)
```

---

# Example

Exclude Cardiology.

```python
df.filter(
~df.Department.isin(
"Cardiology"
)
).show()
```

---

# Visual

```text
All Departments

Remove

Cardiology

      │
      ▼

Remaining Records
```

---

# Intermediate Problem

Exclude:

```text
Critical
High
```

Solution

```python
df.filter(
~df.Severity_Level.isin(
"Critical",
"High"
)
)
```

---

# BETWEEN

---

# What is BETWEEN?

Select records within a range.

---

# Syntax

```python
col.between(start,end)
```

---

# Example

Find:

```text
Age between 30 and 50
```

```python
df.filter(
df.Age.between(30,50)
).show()
```

---

# SQL Equivalent

```sql
WHERE Age BETWEEN 30 AND 50
```

---

# Visual

```text
18 25 30 35 40 45 50 55 60

        └───────┘

Selected
```

---

# Example

Treatment Cost between:

```text
50000 and 150000
```

```python
df.filter(
df.Treatment_Cost.between(
50000,
150000
)
)
```

---

# Advanced Problems

---

## Problem 1

Find patients:

```text
Age > 50

AND

Cost > 100000

AND

Department = Cardiology
```

Solution

```python
df.filter(
(df.Age>50)
&
(df.Treatment_Cost>100000)
&
(df.Department=="Cardiology")
)
```

---

## Problem 2

Find:

```text
Critical OR High Severity

AND

Cost > 150000
```

Solution

```python
df.filter(
(
(df.Severity_Level=="Critical")
|
(df.Severity_Level=="High")
)
&
(df.Treatment_Cost>150000)
)
```

---

## Problem 3

Find:

```text
Age between 40 and 60

Department IN
(
Cardiology,
Neurology
)
```

Solution

```python
df.filter(
df.Age.between(40,60)
&
df.Department.isin(
"Cardiology",
"Neurology"
)
)
```

---

# Practice Assignment

# Easy

### Q1

Age > 30

### Q2

Gender = F

### Q3

Cost > 100000

### Q4

Department = Cardiology

### Q5

Visitors > 5

---

# Solutions

```python
df.filter(df.Age>30)

df.filter(df.Gender=="F")

df.filter(df.Treatment_Cost>100000)

df.filter(df.Department=="Cardiology")

df.filter(df.Visitors_Count>5)
```

---

# Medium

### Q1

Age > 40 AND Male

### Q2

Cost > 50000 AND Cardiology

### Q3

Critical OR High

### Q4

Department IN list

### Q5

Age BETWEEN 30 and 50

---

# Advanced

### Q1

Multiple AND Conditions

### Q2

Multiple OR Conditions

### Q3

IN + BETWEEN

### Q4

NOT IN + Cost Filter

### Q5

Complex Business Logic

---

# Common Mistakes

### Mistake 1

Using Python and/or

❌ Wrong

```python
(df.Age>40) and (df.Gender=="M")
```

---

✅ Correct

```python
(df.Age>40) & (df.Gender=="M")
```

---

### Mistake 2

Forgetting Parentheses

❌ Wrong

```python
df.Age>40 & df.Gender=="M"
```

---

✅ Correct

```python
(df.Age>40) &
(df.Gender=="M")
```

---

# Best Practices

✅ Filter early

✅ Reduce data before joins

✅ Use IN instead of multiple OR conditions

✅ Apply selective filters first

✅ Always use parentheses

---

# Interview Questions

### Q1

Difference between where() and filter()?

Answer:

No difference.

---

### Q2

How to implement SQL IN?

```python
col.isin()
```

---

### Q3

How to implement NOT IN?

```python
~col.isin()
```

---

### Q4

Difference between AND and OR?

```text
AND → All conditions

OR → Any condition
```

---

### Q5

Why filter early in ETL pipelines?

Answer:

Reduces:

* Shuffle
* Memory
* Processing Time

Improves performance.

---

# MODULE 4 REVISION SHEET

```text
where()
filter()

AND → &

OR → |

IN → isin()

NOT IN → ~isin()

BETWEEN → between()
```

**Module 5:**


# MODULE 5: SORTING DATA IN SPARK DATAFRAMES

---

# Why Sorting is Important?

In real-world Data Engineering and Analytics projects, sorting is used everywhere.

Examples:

### Healthcare

Find patients with highest treatment cost.

### Banking

Find top customers by account balance.

### E-Commerce

Find highest selling products.

### Netflix

Find most watched movies.

### Uber

Find drivers with highest ratings.

---

# Visual Understanding

Unsorted Data

| Patient |   Cost |
| ------- | -----: |
| Ram     |  50000 |
| Ravi    | 120000 |
| Priya   |  80000 |

↓

Ascending Sort

| Patient |   Cost |
| ------- | -----: |
| Ram     |  50000 |
| Priya   |  80000 |
| Ravi    | 120000 |

↓

Descending Sort

| Patient |   Cost |
| ------- | -----: |
| Ravi    | 120000 |
| Priya   |  80000 |
| Ram     |  50000 |

---

# Topics Covered

## 1. orderBy()

## 2. sort()

## 3. Ascending Order

## 4. Descending Order

## 5. Multiple Column Sorting

## 6. Real Business Examples

## 7. Optimization Notes

## 8. Interview Questions

---

# 1. orderBy()

---

## What is orderBy()?

Used to sort a DataFrame based on one or more columns.

---

## Syntax

```python
df.orderBy("column_name")
```

---

## Example Dataset

```python
data = [
(101,"Ram","Cardiology",50000),
(102,"Ravi","Neurology",120000),
(103,"Priya","Cardiology",80000)
]

columns = [
"patient_id",
"name",
"department",
"cost"
]
```

---

## Sort by Cost

```python
df.orderBy("cost").show()
```

Output

```text
+----------+------+-----------+------+
|patient_id|name  |department |cost  |
+----------+------+-----------+------+
|101       |Ram   |Cardiology |50000 |
|103       |Priya |Cardiology |80000 |
|102       |Ravi  |Neurology  |120000|
+----------+------+-----------+------+
```

---

# 2. sort()

---

## What is sort()?

sort() and orderBy() are identical.

---

### Syntax

```python
df.sort("cost")
```

---

### Example

```python
df.sort("cost").show()
```

Same result as:

```python
df.orderBy("cost")
```

---

# Interview Question

### Difference Between sort() and orderBy()?

Answer:

```text
No Difference
```

Both perform identical operations.

---

# Ascending Sorting

---

Ascending means:

```text
Small → Large
A → Z
Old → New
```

---

## Default Behavior

Spark sorts in ascending order by default.

```python
df.orderBy("cost")
```

Equivalent to:

```python
df.orderBy("cost", ascending=True)
```

---

## Example

```python
df.orderBy(
    "cost",
    ascending=True
).show()
```

Output

```text
50000
80000
120000
```

---

# Beginner Problems

---

### Problem 1

Sort patients by Age.

Solution

```python
df.orderBy("age").show()
```

---

### Problem 2

Sort treatment cost.

```python
df.orderBy("cost").show()
```

---

### Problem 3

Sort city names.

```python
df.orderBy("city").show()
```

---

# Descending Sorting

---

Descending means:

```text
Large → Small
Z → A
Newest → Oldest
```

---

## Syntax

```python
df.orderBy(
    "cost",
    ascending=False
)
```

---

## Example

Highest Treatment Cost First

```python
df.orderBy(
    "cost",
    ascending=False
).show()
```

Output

```text
120000
80000
50000
```

---

## Alternative Method

Import desc()

```python
from pyspark.sql.functions import desc
```

---

```python
df.orderBy(
desc("cost")
).show()
```

---

# Real Business Example

Top 10 Expensive Treatments

```python
df.orderBy(
desc("cost")
).show(10)
```

---

# Intermediate Problems

---

### Problem 1

Find top 5 oldest patients.

```python
df.orderBy(
desc("age")
).show(5)
```

---

### Problem 2

Find highest revenue hospitals.

```python
df.orderBy(
desc("hospital_revenue")
)
```

---

### Problem 3

Find most visited hospitals.

```python
df.orderBy(
desc("visitors")
)
```

---

# Multiple Column Sorting

---

Real-world sorting rarely happens on a single column.

---

## Example

Sort by:

```text
Department Ascending

Then

Cost Descending
```

---

## Data

```text
Cardiology 120000
Cardiology 80000
Neurology 95000
Neurology 60000
```

---

## Code

```python
df.orderBy(
["department","cost"],
ascending=[True,False]
)
```

---

Output

```text
Cardiology 120000
Cardiology 80000

Neurology 95000
Neurology 60000
```

---

# Visual

```text
Department

Cardiology
    Cost ↓

Neurology
    Cost ↓
```

---

# Advanced Problem

Find:

```text
Department Ascending

Severity Descending

Cost Descending
```

---

Solution

```python
df.orderBy(
["department",
 "severity",
 "cost"],
ascending=[True,False,False]
)
```

---

# Using asc() and desc()

---

Import

```python
from pyspark.sql.functions import asc,desc
```

---

Example

```python
df.orderBy(
asc("department"),
desc("cost")
)
```

---

Production teams commonly use this style.

---

# Top N Records

---

Most common use case.

Find top 10 costly patients.

```python
df.orderBy(
desc("cost")
).limit(10)
```

---

Find Top 5 Hospitals

```python
df.orderBy(
desc("hospital_revenue")
).limit(5)
```

---

# Bottom N Records

Find cheapest treatments.

```python
df.orderBy(
"cost"
).limit(10)
```

---

# Real Project Scenario

Hospital Analytics

Management asks:

### Top 10 Costliest Patients

```python
df.orderBy(
desc("treatment_cost")
).limit(10)
```

---

### Top 5 Departments by Revenue

```python
df.groupBy("department") \
  .sum("treatment_cost") \
  .orderBy(
      desc("sum(treatment_cost)")
  )
```

---

### Most Visited Hospitals

```python
df.groupBy("hospital_id") \
  .count() \
  .orderBy(
      desc("count")
  )
```

---

# Performance Considerations

---

Sorting is expensive.

Why?

Because Spark performs:

```text
Shuffle Operation
```

---

Visual

```text
Executor 1
Executor 2
Executor 3

      ↓

Shuffle

      ↓

Global Sort
```

---

Sorting requires:

* Network Transfer
* Disk Usage
* CPU Usage

---

# Optimization Tips

### Good

Filter First

```python
df.filter(
df.cost > 100000
).orderBy(
desc("cost")
)
```

---

### Bad

```python
df.orderBy(
desc("cost")
).filter(
df.cost > 100000
)
```

Sorts entire dataset first.

Very expensive.

---

# Beginner Practice Assignment

---

### Q1

Sort by Age.

### Solution

```python
df.orderBy("age")
```

---

### Q2

Sort by Cost.

```python
df.orderBy("cost")
```

---

### Q3

Sort by Department.

```python
df.orderBy("department")
```

---

### Q4

Sort by Gender.

```python
df.orderBy("gender")
```

---

### Q5

Sort by City.

```python
df.orderBy("city")
```

---

# Medium Assignment

---

### Q1

Sort Age Descending.

```python
df.orderBy(
desc("age")
)
```

---

### Q2

Sort Cost Descending.

```python
df.orderBy(
desc("cost")
)
```

---

### Q3

Top 10 Costliest Patients.

```python
df.orderBy(
desc("cost")
).limit(10)
```

---

### Q4

Top 5 Hospitals by Revenue.

```python
df.groupBy("hospital_id") \
.orderBy(desc("revenue"))
```

---

### Q5

Department Asc + Cost Desc

```python
df.orderBy(
["department","cost"],
ascending=[True,False]
)
```

---

# Advanced Assignment

---

### Q1

Department Asc

Severity Desc

Cost Desc

```python
df.orderBy(
["department","severity","cost"],
ascending=[True,False,False]
)
```

---

### Q2

Top 3 Patients per Department

(Will be solved properly using Window Functions later)

---

### Q3

Highest Revenue Hospital per City

---

### Q4

Sort Aggregated Revenue Data

---

### Q5

Multi-Level Business Ranking

---

# Common Mistakes

---

### Mistake 1

Using Python sorted()

❌ Wrong

```python
sorted(df)
```

---

✅ Correct

```python
df.orderBy("cost")
```

---

### Mistake 2

Sorting Before Filtering

❌

```python
df.orderBy("cost") \
.filter(df.cost > 100000)
```

---

✅

```python
df.filter(
df.cost > 100000
).orderBy("cost")
```

---

### Mistake 3

Forgetting desc()

```python
df.orderBy("cost")
```

Gives ascending.

Use:

```python
desc("cost")
```

---

# Interview Questions

---

### Q1

Difference between sort() and orderBy()?

Answer:

```text
No Difference
```

---

### Q2

Default sorting order?

Answer:

```text
Ascending
```

---

### Q3

How do you sort descending?

```python
desc("column")
```

or

```python
ascending=False
```

---

### Q4

Why is sorting expensive?

Answer:

```text
Shuffle Operation
```

---

### Q5

How to get Top N records?

```python
df.orderBy(
desc("column")
).limit(N)
```

---

# MODULE 5 REVISION SHEET

```text
orderBy()

sort()

asc()

desc()

limit()

Single Column Sorting

Multiple Column Sorting

ascending=True

ascending=False

Shuffle Operation

Filter Before Sort
```

# What You Can Do Now

You can now:

✅ Sort by one column

✅ Sort by multiple columns

✅ Ascending sorting

✅ Descending sorting

✅ Top-N analysis

✅ Production-level sorting optimization

---

Type **NEXT MODULE** for:

# MODULE 6: FEATURE ENGINEERING

* withColumn()
* expr()
* when()
* otherwise()
* lit()
* UDF()

This is one of the most important modules for Data Engineering, Spark Interviews, and Real-World ETL Pipelines.

