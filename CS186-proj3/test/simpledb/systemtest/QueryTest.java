package simpledb.systemtest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.DbException;
import simpledb.HeapFile;
import simpledb.HeapFileEncoder;
import simpledb.Parser;
import simpledb.TableStats;
import simpledb.Transaction;
import simpledb.TransactionAbortedException;
import simpledb.Utility;

public class QueryTest {
	
	/**
	 * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create an identical HeapFile table
	 * @param tuples Tuples to create a HeapFile from
	 * @param columns Each entry in tuples[] must have "columns == tuples.get(i).size()"
	 * @param colPrefix String to prefix to the column names (the columns are named after their column number by default)
	 * @return a new HeapFile containing the specified tuples
	 * @throws IOException if a temporary file can't be created to hand to HeapFile to open and read its data
	 */
	public static HeapFile createDuplicateHeapFile(ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix) throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.PAGE_SIZE, columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
	}
	
	@Test(timeout=20000) public void queryTest() throws IOException, DbException, TransactionAbortedException {
		final int IO_COST = 101;
		
	
		// Create all of the tables, and add them to the catalog
		ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 1000, null, empTuples, "c");	
		Database.getCatalog().addTable(emp, "emp");
		
		ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 100, null, deptTuples, "c");	
		Database.getCatalog().addTable(dept, "dept");
		
		ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 100, null, hobbyTuples, "c");
		Database.getCatalog().addTable(hobby, "hobby");
		
		ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 2000, null, hobbiesTuples, "c");
		Database.getCatalog().addTable(hobbies, "hobbies");
		
		// Get TableStats objects for each of the tables that we just generated.
		TableStats.setTableStats("emp", new TableStats(Database.getCatalog().getTableId("emp"), IO_COST));
		TableStats.setTableStats("dept", new TableStats(Database.getCatalog().getTableId("dept"), IO_COST));
		TableStats.setTableStats("hobby", new TableStats(Database.getCatalog().getTableId("hobby"), IO_COST));
		TableStats.setTableStats("hobbies", new TableStats(Database.getCatalog().getTableId("hobbies"), IO_COST));
	
		Transaction t = new Transaction();
		t.start();
		Parser p = new Parser();
		p.setTransaction(t);
		
		p.processNextStatement("SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND emp.c3 < 1000;");
	}	
}
