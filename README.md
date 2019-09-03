# SimpleDB
Database Homework of Berkeley University: Implement A Simple Database Management System 

**You can get more details in https://sites.google.com/site/cs186fall2013/homeworks**  
 **and my blog(Chinese version): https://blog.csdn.net/xpy870663266/article/details/78163423 (see the English version in README)**
 

## API Changes
+ In Pro1, I change parameters of several methods
+ In Pro3, I change the method computeCostAndCardOfSubplan() in JoinOptimizer.java in order to get a left-deep-tree(I might have found some bugs in the code provided by the course). Besides, I change the method physicalPlan() in LogicalPlan.java. Specifically, the code I added will check if the Query contains a Join, and if not, the JoinOptimizer won't be invoked.


## English version of my Blog

I finished the Database Homework of Berkeley University, i.e. implementing SimpleDB, in my spare time. This blog is just intended to share my experience and recommend the course to more friends.

### Outline of the Course

The course consists of four sections that implement four core parts in the database:

**Project 1**: Implement data management. This part is mainly to realize the management of data. Of course, it is necessary to set up the development environment and understand the overall framework of SimpleDB under the guidance. More specifically, it is necessary to implement storage, access and management of physical level data (binary files) and map it to logical level data (relational tables). At the end of this project, the most basic operation in SimpleDB, SeqScan, is also required. So after completing this project, you can scan the entire table.

**Project 2**: Implement the operators. This part mainly implements a series of operators, including `insert`, `delete`, `select`, `join`, `group by`, `order by`, and so on. It is worth noting that implementing a highly efficienct version of `join` is the main and difficult question (but don't worry, we just need to learn some common algorithms, there are recommended articles at the end of this article). Plus, the `group by` and `order by` functions in SimpleDB have been simplified, so some work is actually saved. At the end of the project, we need to implement the cache management, a function that is not completed in project 1. You will learn and implement the caching mechanism, including the cache replacement strategies (LRU, etc.).

### CS186 Project 2: SimpleDB Operators
In this project, you will write a set of operators for SimpleDB to implement table modifications (e.g., insert and delete records), selections, joins, and aggregates. These will build on top of the foundation that you wrote in Project 1 to provide you with a database system that can perform simple queries over multiple tables.

Additionally, we ignored the issue of buffer pool management in Project 1: we have not dealt with the problem that arises when we reference more pages than we can fit in memory over the lifetime of the database. In Project 2, you will design an eviction policy to flush stale pages from the buffer pool.

You do not need to implement transactions or locking in this project.

### CS186 Project 3: Query Optimization
In this project, you will implement a query optimizer on top of SimpleDB. The main tasks include implementing a selectivity estimation framework and a cost-based optimizer. You have freedom as to exactly what you implement, but we recommend using something similar to the Selinger cost-based optimizer discussed in class. The remainder of this document describes what is involved in adding optimizer support and provides a basic outline of how you might add this support to your database. 

### CS186 Project 4: SimpleDB Transactions
In this project, you will implement a simple locking-based transaction system in SimpleDB. You will need to add lock and unlock calls at the appropriate places in your code, as well as code to track the locks held by each transaction and grant locks to transactions as they are needed. 

