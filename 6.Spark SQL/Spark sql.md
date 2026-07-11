# Apache Spark SQL – Complete Notes

## What is Spark SQL?

**Spark SQL** is a module of Apache Spark used to process **structured** and **semi-structured** data using SQL queries and the DataFrame API.

It provides a programming abstraction (DSL - Domain-Specific Language) for data processing and combines the power of SQL with Spark's distributed computing.

---

# Domain-Specific Language (DSL)

A **Domain-Specific Language (DSL)** is a programming language designed for a specific purpose.

### Examples

| DSL  | Purpose                        |
| ---- | ------------------------------ |
| SQL  | Querying and manipulating data |
| HTML | Creating web pages             |
| CSS  | Styling web pages              |

Spark SQL provides a DSL specifically for querying and processing structured data.

---

# Components of Spark SQL

## 1. Hive Metastore Integration

Spark SQL can connect to the **Apache Hive Metastore**.

This allows Spark to:

* Access existing Hive tables
* Use Hive metadata
* Query Hive databases without moving data

Spark SQL also provides an interactive SQL shell for querying Hive tables.

---

## 2. DataFrame API

A **DataFrame** is a distributed collection of data organized into **rows and named columns**.

It is similar to a **table in a relational database (SQL table).**

### Example

| Emp_ID | Name  | Salary |
| ------ | ----- | ------ |
| 101    | Alice | 50000  |
| 102    | Bob   | 60000  |

In Spark:

```python
df.show()
```

### Advantages

* Schema-based
* Optimized execution
* Easy to query using SQL
* Fault tolerant

---

## 3. DataSource API

The **DataSource API** is a universal API used for loading and storing structured data.

It supports many data sources.

### Supported Formats

* CSV
* JSON
* Parquet
* ORC
* Avro
* JDBC
* Hive Tables
* Text Files

### Example

```python
df = spark.read.csv("employees.csv", header=True)
```

```python
df = spark.read.json("students.json")
```

---

# Applications of Spark SQL

Spark SQL is widely used across industries.

### Banking

* Credit card fraud detection
* Customer analytics
* Risk analysis

### Stock Market

* Real-time stock analysis
* Trading analytics

### Healthcare

* Genomic sequencing
* Medical data processing

### E-Commerce

* Customer behavior analysis
* Recommendation systems

### Telecommunications

* Call detail record analysis
* Network monitoring

---

# Features of Spark SQL

## 1. Integrative

Spark SQL allows SQL queries to be mixed with Spark programs.

Supported Languages

* Python
* Scala
* Java
* R

### Example

```python
df.createOrReplaceTempView("employees")

spark.sql("""
SELECT *
FROM employees
WHERE salary > 50000
""").show()
```

This combines SQL with Python Spark code.

---

## 2. Hive Compatibility

Spark SQL is compatible with Apache Hive.

Benefits:

* Read Hive tables
* Use existing Hive metadata
* Execute Hive SQL queries
* No need to migrate Hive data

---

## 3. High Performance and Scalability

Spark SQL is highly optimized.

It uses an internal optimizer called the **Catalyst Optimizer**.

Benefits:

* Faster execution
* Automatic query optimization
* Efficient resource utilization
* Better scalability for large datasets

---

## 4. Standard Connectivity

Spark SQL supports standard database connectivity using:

* JDBC
* ODBC

This allows BI tools and external applications to connect to Spark.

Examples:

* Tableau
* Power BI
* Excel
* SQL Clients

---

## 5. Uniform Data Access

Spark SQL provides a common interface to access multiple data sources.

Supported Sources

* Hive
* JSON
* CSV
* ORC
* Parquet
* Avro
* JDBC Databases

It can also join data across different sources.

### Example

```
CSV File
     \
      \
       ---- Spark SQL ----> Joined Result
      /
Hive /
    /
JDBC Database
```

---

# Why is Spark SQL Fast?

Spark SQL automatically optimizes queries before execution using the **Catalyst Optimizer**.

The optimizer:

* Analyzes SQL queries
* Removes unnecessary operations
* Reorders operations for better performance
* Generates an optimized execution plan

As a result:

* Faster query execution
* Reduced memory usage
* Better CPU utilization
* Improved scalability

---

# Spark SQL Architecture (Simple Flow)

```
User

   │
   ▼

Spark SQL Query
or
DataFrame API

   │
   ▼

Catalyst Optimizer
(Query Optimization)

   │
   ▼

Optimized Logical Plan

   │
   ▼

Physical Plan

   │
   ▼

Spark Execution Engine

   │
   ▼

Results
```

---

# Advantages of Spark SQL

* Easy to write SQL queries
* Supports SQL and DataFrames
* Compatible with Hive
* Works with multiple data formats
* Automatic query optimization
* Highly scalable
* Distributed processing
* Supports JDBC/ODBC connectivity
* Can join data from multiple sources
* Integrates with Python, Scala, Java, and R

---

# Interview Questions

### 1. What is Spark SQL?

Spark SQL is a Spark module used to process structured and semi-structured data using SQL queries and the DataFrame API.

---

### 2. What is a DataFrame?

A DataFrame is a distributed collection of data organized into rows and named columns, similar to a relational database table.

---

### 3. What is the DataSource API?

The DataSource API is a universal API for reading and writing structured data from sources such as CSV, JSON, Parquet, ORC, Hive, and JDBC databases.

---

### 4. What are the features of Spark SQL?

* Integrative (SQL + Spark code)
* Hive compatibility
* High performance using Catalyst Optimizer
* JDBC/ODBC connectivity
* Uniform data access

---

### 5. What is the difference between SQL and Spark SQL?

| SQL                           | Spark SQL                                  |
| ----------------------------- | ------------------------------------------ |
| Works on a single database    | Works on distributed data across a cluster |
| Limited scalability           | Highly scalable                            |
| Suitable for smaller datasets | Designed for big data                      |
| Runs on database servers      | Runs on Apache Spark                       |

---

# Quick Revision

* **Spark SQL** → Module for processing structured and semi-structured data.
* **DSL** → Domain-Specific Language for a specific purpose (e.g., SQL, HTML, CSS).
* **DataFrame** → Distributed table with named columns.
* **DataSource API** → Reads/writes CSV, JSON, Parquet, ORC, Hive, JDBC, etc.
* **Hive Compatibility** → Access Hive tables and metadata.
* **Integrative** → Mix SQL with Python, Scala, Java, or R.
* **Standard Connectivity** → Supports JDBC and ODBC.
* **Uniform Data Access** → One API for multiple data sources.
* **Catalyst Optimizer** → Automatically optimizes queries for better performance.
