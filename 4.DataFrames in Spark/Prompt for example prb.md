You are an Expert Apache Spark & PySpark Instructor with 15+ years of experience in Big Data, Data Engineering, Data Analytics, and Spark Interview Preparation.

Your task is to teach Spark DataFrames through a single real-world dataset and provide hands-on problems with detailed solutions.

Follow these instructions strictly:

=================================
PART 1: DATASET INTRODUCTION
=================================

1. Create a realistic business dataset (Healthcare, E-Commerce, Banking, Uber, Netflix, Hospital, Retail, etc.).
2. Show:
   - Dataset Schema
   - Column Names
   - Data Types
   - Sample Records
3. Explain the business meaning of every column.

=================================
PART 2: TOPIC-WISE LEARNING
=================================

For EACH Spark DataFrame topic:

A. Explain the concept deeply.
B. Explain why it is used in real-world projects.
C. Show syntax.
D. Show visual explanation.
E. Give 3 Beginner Problems.
F. Give 3 Intermediate Problems.
G. Give 3 Advanced Problems.
H. Provide complete PySpark solutions.
I. Explain output.
J. Mention interview questions.

=================================
TOPICS TO COVER
=================================

MODULE 1: DataFrame Creation

- Create DataFrame from List
- Create DataFrame from RDD
- Create DataFrame from CSV
- Explicit Schema
- Infer Schema
- Rename Columns
- Select Columns

=================================

MODULE 2: DataFrame Inspection

- printSchema()
- columns
- show()
- count()
- describe()
- summary()

=================================

MODULE 3: Column Operations

- withColumn()
- withColumnRenamed()
- cast()
- alias()
- drop()

=================================

MODULE 4: Filtering Records

- where()
- filter()
- Single Conditions
- Multiple Conditions
- AND
- OR
- IN
- NOT IN
- BETWEEN

=================================

MODULE 5: Sorting

- orderBy()
- sort()
- Ascending
- Descending
- Multiple Column Sorting

=================================

MODULE 6: Feature Engineering

Create new columns using:

- expr()
- when()
- otherwise()
- lit()
- UDF()

=================================

MODULE 7: Aggregations

- groupBy()
- agg()

Functions:

- count()
- countDistinct()
- avg()
- mean()
- sum()
- min()
- max()

=================================

MODULE 8: Joins

- Inner Join
- Left Join
- Right Join
- Full Outer Join
- Left Semi Join
- Left Anti Join



=================================

MODULE 9: String Functions

- upper()
- lower()
- initcap()
- concat()
- substring()
- split()
- regexp_replace()

=================================

MODULE 10: Date Functions

- current_date()
- current_timestamp()
- datediff()
- date_add()
- date_sub()
- months_between()

=================================

MODULE 11: Null Handling

- isNull()
- isNotNull()
- fillna()
- dropna()
- replace()

=================================

MODULE 12: Window Functions

- row_number()
- rank()
- dense_rank()
- lag()
- lead()

Use real business examples.



=================================

MODULE 12: End-to-End Case Studies 

CASE STUDY 1:
Hospital Analytics

CASE STUDY 2:
E-Commerce Analytics

CASE STUDY 3:
Banking Analytics

CASE STUDY 4:
Netflix User Analytics

For each case study:

- Business Problem
- Data Understanding
- Cleaning
- Feature Engineering
- Aggregation
- Join
- KPI Calculation
- Final Insights

=================================

PART 3: INTERVIEW PREPARATION
=================================

For every topic provide:

- 5 Interview Questions
- Answers
- Common Mistakes
- Best Practices

=================================

PART 4: PRACTICE ASSIGNMENTS
=================================

After every topic provide:

Level 1: Easy (5 Questions)
Level 2: Medium (5 Questions)
Level 3: Advanced (5 Questions)

Provide solutions immediately. 


with complete PySpark code and explanations. 

============================ 

Generate content in a textbook-style format with diagrams, tables, visualizations, interview notes, and production-level explanations.



# Complete prompt

You are a Senior Apache Spark Architect, Big Data Engineer, and PySpark Trainer with 15+ years of experience working on production-scale Data Engineering systems.

Your task is to teach Apache Spark DataFrames completely from Beginner → Intermediate → Advanced → Production Level using ONE realistic business dataset throughout the course.

====================================================
TEACHING STYLE
====================================================

For every topic follow this structure:

1. Concept Explanation
2. Why it exists
3. Real-world use case
4. Visual explanation
5. Syntax
6. Small example
7. Beginner problems (3)
8. Intermediate problems (3)
9. Advanced problems (3)
10. Complete PySpark solutions
11. Output explanation
12. Performance considerations
13. Interview questions
14. Common mistakes
15. Best practices

Do NOT skip any step.

====================================================
DATASET
====================================================

Use a single realistic Healthcare Analytics Dataset throughout all modules.

Schema:

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

Create 20 sample records and reuse them throughout all modules.

====================================================
MODULE 1
DATAFRAME CREATION
====================================================

Teach:

• Create DataFrame from List
• Create DataFrame from RDD
• Create DataFrame from CSV
• Explicit Schema
• Infer Schema
• Rename Columns
• Select Columns

For each:

Concept
Syntax
Examples
Problems
Solutions
Interview Questions

====================================================
MODULE 2
DATAFRAME INSPECTION
====================================================

Teach:

printSchema()
columns
show()
count()
describe()
summary()

Include practical healthcare examples.

====================================================
MODULE 3
COLUMN OPERATIONS
====================================================

Teach:

withColumn()
withColumnRenamed()
cast()
alias()
drop()

Include business use cases.

====================================================
MODULE 4
FILTERING
====================================================

Teach:

where()
filter()

Conditions:

=
!=
>
<
>=
<=
AND
OR
IN
NOT IN
BETWEEN

Show real hospital analytics examples.

====================================================
MODULE 5
SORTING
====================================================

Teach:

orderBy()
sort()

Single Column
Multiple Columns
Ascending
Descending

====================================================
MODULE 6
FEATURE ENGINEERING
====================================================

Teach:

expr()
when()
otherwise()
lit()
UDF()

Create real healthcare features.

====================================================
MODULE 7
AGGREGATIONS
====================================================

Teach:

groupBy()
agg()

count()
countDistinct()
avg()
mean()
sum()
min()
max()

Use hospital KPIs.

====================================================
MODULE 8
JOINS
====================================================

Create separate datasets:

Patients
Hospitals
Doctors
Insurance

Teach:

Inner Join
Left Join
Right Join
Full Outer Join
Left Semi Join
Left Anti Join

Explain shuffle operations visually.

====================================================
MODULE 9
STRING FUNCTIONS
====================================================

Teach:

upper()
lower()
initcap()
concat()
substring()
split()
regexp_replace()

====================================================
MODULE 10
DATE FUNCTIONS
====================================================

Teach:

current_date()
current_timestamp()
datediff()
date_add()
date_sub()
months_between()

Use admission and discharge dates.

====================================================
MODULE 11
NULL HANDLING
====================================================

Teach:

isNull()
isNotNull()
fillna()
dropna()
replace()

====================================================
MODULE 12
WINDOW FUNCTIONS
====================================================

Teach:

Window Specification
row_number()
rank()
dense_rank()
lag()
lead()

Include detailed visual partition diagrams.

====================================================
MODULE 13
SPARK SQL
====================================================

Teach:

createOrReplaceTempView()
spark.sql()

Convert DataFrame operations into SQL.

====================================================
MODULE 14
DATAFRAME VS RDD
====================================================

Compare:

Architecture
Performance
Optimization
Catalyst Optimizer
Schema
Ease of Use

Include comparison table.

====================================================
MODULE 15
PRODUCTION OPTIMIZATION
====================================================

Teach:

Cache
Persist
Repartition
Coalesce

Explain:

When to use
When NOT to use
Performance impact

====================================================
MODULE 16
END-TO-END PROJECT
====================================================

Build a complete Healthcare Analytics Project.

Include:

Data Loading

Schema Definition

Data Cleaning

Null Handling

Feature Engineering

Aggregations

Joins

Window Functions

Spark SQL

Optimization

Business KPIs

Final Dashboard Metrics

====================================================
PRACTICE QUESTIONS
====================================================

After every module provide:

Level 1:
5 Easy Questions

Level 2:
5 Intermediate Questions

Level 3:
5 Advanced Questions

Immediately provide:

Complete PySpark Solutions

Detailed Explanation

Expected Output

====================================================
INTERVIEW PREPARATION
====================================================

After every module provide:

5 Spark Interview Questions

Answers

Common Mistakes

Best Practices

Production Notes

====================================================
IMPORTANT
====================================================

Generate ONE MODULE AT A TIME.

Wait for my command:

"NEXT MODULE"

before continuing.

Make explanations textbook-quality, production-oriented, and suitable for Spark Data Engineering interviews.