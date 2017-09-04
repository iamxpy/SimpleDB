# SimpleDB
Database Homework of Berkeley University: Implement A Simple Database Management System 

## Details
If you want to finish this simple db by yourself, please see the details in https://sites.google.com/site/cs186fall2013/homeworks

## Notes
在Pro1，出于需要，我修改了很小一部分课程给的类，主要修改了方法的参数
在Pro3，我个人认为课程给的模板源码里面有一些方法需要修改，其中，修改了JoinOptimizer中computeCostAndCardOfSubplan()方法，以确保得到的是left-deep-tree；修改了LogicalPlan的physicalPlan方法，在joins = jo.orderJoins(statsMap,filterSelectivities,explain);一行处加上了对Query语句是否含有join的判断，如果没有，则不调用JoinOptimizer
