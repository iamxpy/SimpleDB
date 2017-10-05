# SimpleDB
Database Homework of Berkeley University: Implement A Simple Database Management System 

## Details
**You can get more details on https://sites.google.com/site/cs186fall2013/homeworks**

### CS186 Project 1: SimpleDB
In the project assignments in CS186, you will write a basic database management system called SimpleDB. For this project, you will focus on implementing the core modules required to access stored data on disk; in future projects, you will add support for various query processing operators, as well as transactions, locking, and concurrent queries.

SimpleDB is written in Java. We have provided you with a set of mostly unimplemented classes and interfaces. You will need to write the code for these classes. We will grade your code by running a set of system tests written using JUnit. We have also provided a number of unit tests that you may find useful in verifying that your code works.

### CS186 Project 2: SimpleDB Operators
In this project, you will write a set of operators for SimpleDB to implement table modifications (e.g., insert and delete records), selections, joins, and aggregates. These will build on top of the foundation that you wrote in Project 1 to provide you with a database system that can perform simple queries over multiple tables.

Additionally, we ignored the issue of buffer pool management in Project 1: we have not dealt with the problem that arises when we reference more pages than we can fit in memory over the lifetime of the database. In Project 2, you will design an eviction policy to flush stale pages from the buffer pool.

You do not need to implement transactions or locking in this project.

### CS186 Project 3: Query Optimization
In this project, you will implement a query optimizer on top of SimpleDB. The main tasks include implementing a selectivity estimation framework and a cost-based optimizer. You have freedom as to exactly what you implement, but we recommend using something similar to the Selinger cost-based optimizer discussed in class. The remainder of this document describes what is involved in adding optimizer support and provides a basic outline of how you might add this support to your database. 

### CS186 Project 4: SimpleDB Transactions
In this project, you will implement a simple locking-based transaction system in SimpleDB. You will need to add lock and unlock calls at the appropriate places in your code, as well as code to track the locks held by each transaction and grant locks to transactions as they are needed. 

## Changes to the API
+ In Pro1, I change several methods' parameters out of necessity
+ In Pro3, I think I probably find some bugs in the code provided by the course, so I change the method computeCostAndCardOfSubplan() in JoinOptimizer.java with the purpose of getting a left-deep-tree. What's more, I change the method physicalPlan() in LogicalPlan.java. Specifically, the code I added will check if the Query contains a Join, if not, the JoinOptimizer wont be invoked.
