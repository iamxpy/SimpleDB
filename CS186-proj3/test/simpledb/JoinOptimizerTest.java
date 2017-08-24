package simpledb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class JoinOptimizerTest extends SimpleDbTestBase {
	
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

	ArrayList<ArrayList<Integer>> tuples1;
	HeapFile f1;
	String tableName1;
	int tableId1;
	TableStats stats1;
	
	ArrayList<ArrayList<Integer>> tuples2;
	HeapFile f2;
	String tableName2;
	int tableId2;
	TableStats stats2;
	
	/**
	 * Set up the test; create some initial tables to work with
	 */
	@Before public void setUp() throws Exception {
		super.setUp();
		// Create some sample tables to work with
		this.tuples1 = new ArrayList<ArrayList<Integer>>();
		this.f1 = SystemTestUtil.createRandomHeapFile(10, 1000, 20, null, tuples1, "c");
		
		this.tableName1 = "TA";
		Database.getCatalog().addTable(f1, tableName1);
		this.tableId1 = Database.getCatalog().getTableId(tableName1);	
		System.out.println("tableId1: " + tableId1);

		stats1 = new TableStats(tableId1, 19);
                TableStats.setTableStats(tableName1, stats1);
		
		this.tuples2 = new ArrayList<ArrayList<Integer>>();
		this.f2 = SystemTestUtil.createRandomHeapFile(10, 10000, 20, null, tuples2, "c");
		
		this.tableName2 = "TB";
		Database.getCatalog().addTable(f2, tableName2);
		this.tableId2 = Database.getCatalog().getTableId(tableName2);
		System.out.println("tableId2: " + tableId2);
		
		stats2 = new TableStats(tableId2, 19);
		
		TableStats.setTableStats(tableName2, stats2);
	}
	
	private double[] getRandomJoinCosts(JoinOptimizer jo, LogicalJoinNode js, int[] card1s, int[] card2s, double[] cost1s, double[] cost2s) {
		double[] ret = new double[card1s.length];
		for(int i = 0; i < card1s.length; ++i) {
			ret[i] = jo.estimateJoinCost(js, card1s[i], card2s[i], cost1s[i], cost2s[i]);
			//assert that he join cost is no less than the total cost of scanning two tables
			Assert.assertTrue(ret[i] > cost1s[i] + cost2s[i]);
		}
		return ret;
	}
	
	/**
	 * Verify that the estimated join costs from estimateJoinCost() are reasonable
	 * we check various order requirements for the output of estimateJoinCost.
	 */
	@Test public void estimateJoinCostTest() throws ParsingException {
		// It's hard to narrow these down much at all, because students 
		// may have implemented custom join algorithms.
		// So, just make sure the orders of the return values make sense.
						
        TransactionId tid = new TransactionId();
        JoinOptimizer jo;
        Parser p = new Parser();
		jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM " + tableName1 + " t1, " + tableName2 + " t2 WHERE t1.c1 = t2.c2;"), 
											new Vector<LogicalJoinNode>());
		// 1 join 2
		LogicalJoinNode equalsJoinNode = new LogicalJoinNode(tableName1, tableName2, Integer.toString(1), Integer.toString(2), Predicate.Op.EQUALS);
		checkJoinEstimateCosts(jo, equalsJoinNode);
		// 2 join 1
		jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM " + tableName1 + " t1, " + tableName2 + " t2 WHERE t1.c1 = t2.c2;"), 
				new Vector<LogicalJoinNode>());
		equalsJoinNode = new LogicalJoinNode(tableName2, tableName1, Integer.toString(2), Integer.toString(1), Predicate.Op.EQUALS);
		checkJoinEstimateCosts(jo, equalsJoinNode);
		// 1 join 1
		jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM " + tableName1 + " t1, " + tableName1 + " t2 WHERE t1.c3 = t2.c4;"), 
				new Vector<LogicalJoinNode>());
		equalsJoinNode = new LogicalJoinNode(tableName1, tableName1, Integer.toString(3), Integer.toString(4), Predicate.Op.EQUALS);
		checkJoinEstimateCosts(jo, equalsJoinNode);	
		// 2 join 2
		jo = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM " + tableName2 + " t1, " + tableName2 + " t2 WHERE t1.c8 = t2.c7;"), 
				new Vector<LogicalJoinNode>());
		equalsJoinNode = new LogicalJoinNode(tableName2, tableName2, Integer.toString(8), Integer.toString(7), Predicate.Op.EQUALS);
		checkJoinEstimateCosts(jo, equalsJoinNode);		
	}
	
	private void checkJoinEstimateCosts(JoinOptimizer jo,
			LogicalJoinNode equalsJoinNode) {
		int card1s[] = new int[20]; 
		int card2s[] = new int[card1s.length];
		double cost1s[] = new double[card1s.length]; 
		double cost2s[] = new double[card1s.length];
		Object[] ret;
		//card1s linear others constant
		for(int i = 0; i < card1s.length; ++i) {
			card1s[i] = 3*i+1; card2s[i] = 5; cost1s[i] = cost2s[i] =5.0;			
		}
		double stats[] = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s, cost2s);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals(Boolean.TRUE, ret[0]);
		//card2s linear others constant
		for(int i = 0; i < card1s.length; ++i) {
			card1s[i] = 4; card2s[i] = 3*i+1; cost1s[i] = cost2s[i] = 5.0;			
		}
		stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s, cost2s);
		ret = SystemTestUtil.checkLinear(stats); 
		Assert.assertEquals(Boolean.TRUE, ret[0]);
		//cost1s linear others constant
		for(int i = 0; i < card1s.length; ++i) {
			card1s[i] = card2s[i] = 7; cost1s[i] = 5.0*(i+1); cost2s[i] = 3.0;			
		}
		stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s, cost2s);
		ret = SystemTestUtil.checkLinear(stats); 
		Assert.assertEquals(Boolean.TRUE, ret[0]);
		//cost2s linear others constant
		for(int i = 0; i < card1s.length; ++i) {
			card1s[i] = card2s[i] = 9; cost1s[i] = 5.0; cost2s[i] = 3.0*(i+1);			
		}
		stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s, cost2s);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals( Boolean.TRUE, ret[0]);
		//everything linear
		for(int i = 0; i < card1s.length; ++i) {
			card1s[i] = 2*(i+1); card2s[i] = 9*i + 1; cost1s[i] = 5.0*i + 2; cost2s[i] = 3.0*i + 1;			
		}
		stats = getRandomJoinCosts(jo, equalsJoinNode, card1s, card2s, cost1s, cost2s);
		ret = SystemTestUtil.checkQuadratic(stats);
		Assert.assertEquals(Boolean.TRUE, ret[0]);
	}

	/**
	 * Verify that the join cardinalities produced by estimateJoinCardinality() are reasonable
	 */
	@Test public void estimateJoinCardinality() throws ParsingException {
        TransactionId tid = new TransactionId();
        Parser p = new Parser();
		JoinOptimizer j = new JoinOptimizer(p.generateLogicalPlan(tid, "SELECT * FROM " + tableName2 + " t1, " + tableName2 + " t2 WHERE t1.c8 = t2.c7;"), 
		new Vector<LogicalJoinNode>());

		double cardinality;
		
                /* Disable these tests as almost any answer could be defensible

		cardinality = j.estimateJoinCardinality(new LogicalJoinNode(tableName1, tableName2, Integer.toString(3), Integer.toString(4), Predicate.Op.EQUALS),
												stats1.estimateTableCardinality(0.8), stats2.estimateTableCardinality(0.2), false, false);
		
		// We don't specify in what way statistics should be used to improve these estimates.
		// So, just require that they not be entirely unreasonable.
		Assert.assertTrue(cardinality > 800);
		Assert.assertTrue(cardinality <= 2000);
		
		cardinality = j.estimateJoinCardinality(new LogicalJoinNode(tableName2, tableName1, Integer.toString(3), Integer.toString(4), Predicate.Op.EQUALS),
												stats2.estimateTableCardinality(0.2), stats1.estimateTableCardinality(0.8), false, false);

		Assert.assertTrue(cardinality > 800);
		Assert.assertTrue(cardinality <= 2000);
                */

		cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2", "c"+Integer.toString(3), "c"+Integer.toString(4), Predicate.Op.EQUALS),
												stats1.estimateTableCardinality(0.8), stats2.estimateTableCardinality(0.2), true, false, TableStats.getStatsMap());

		// On a primary key, the cardinality is well-defined and exact (should be size of fk table)
                //   BUT we had a bug in lab 4 in 2009 that suggested should be size of pk table, so accept either
                Assert.assertTrue(cardinality == 800 || cardinality == 2000);

		cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2", "c"+Integer.toString(3), "c"+Integer.toString(4), Predicate.Op.EQUALS),
												stats1.estimateTableCardinality(0.8), stats2.estimateTableCardinality(0.2), false, true,TableStats.getStatsMap());

	         Assert.assertTrue(cardinality == 800 || cardinality == 2000);
	}
	
	/**
	 * Determine whether the orderJoins implementation is doing a reasonable job of ordering joins,
	 * and not taking an unreasonable amount of time to do so 
	 */
	@Test public void orderJoinsTest() throws ParsingException, IOException, DbException, TransactionAbortedException {
		// This test is intended to approximate the join described in the
		// "Query Planning" section of 2009 Quiz 1,
		// though with some minor variation due to limitations in simpledb
		// and to only test your integer-heuristic code rather than
		// string-heuristic code.
		
		final int IO_COST = 101;
		
		// Create a whole bunch of variables that we're going to use
		TransactionId tid = new TransactionId();
		JoinOptimizer j;
		Vector<LogicalJoinNode> result;
		Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
		HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
		HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();
		
		// Create all of the tables, and add them to the catalog
		ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null, empTuples, "c");	
		Database.getCatalog().addTable(emp, "emp");
		
		ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null, deptTuples, "c");	
		Database.getCatalog().addTable(dept, "dept");
		
		ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null, hobbyTuples, "c");
		Database.getCatalog().addTable(hobby, "hobby");
		
		ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null, hobbiesTuples, "c");
		Database.getCatalog().addTable(hobbies, "hobbies");
		
		// Get TableStats objects for each of the tables that we just generated.
		stats.put("emp", new TableStats(Database.getCatalog().getTableId("emp"), IO_COST));
		stats.put("dept", new TableStats(Database.getCatalog().getTableId("dept"), IO_COST));
		stats.put("hobby", new TableStats(Database.getCatalog().getTableId("hobby"), IO_COST));
		stats.put("hobbies", new TableStats(Database.getCatalog().getTableId("hobbies"), IO_COST));
		
		// Note that your code shouldn't re-compute selectivities.
		// If you get statistics numbers, even if they're wrong (which they are here
		// because the data is random), you should use the numbers that you are given.
		// Re-computing them at runtime is generally too expensive for complex queries.
		filterSelectivities.put("emp", 0.1);
		filterSelectivities.put("dept", 1.0);
		filterSelectivities.put("hobby", 1.0);
		filterSelectivities.put("hobbies", 1.0);
		
		// Note that there's no particular guarantee that the LogicalJoinNode's will be in
		// the same order as they were written in the query.
		// They just have to be in an order that uses the same operators and 
		// semantically means the same thing.
		nodes.add(new LogicalJoinNode("hobbies", "hobby", "c1", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("emp", "dept", "c1", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("emp", "hobbies", "c2", "c0", Predicate.Op.EQUALS));
		Parser p = new Parser();
		j = new JoinOptimizer(
				p.generateLogicalPlan(tid, "SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND e.c3 < 1000;"),
				nodes);

		// Set the last boolean here to 'true' in order to have orderJoins() print out its logic
		result = j.orderJoins(stats, filterSelectivities, false);
		
		// There are only three join nodes; if you're only re-ordering the join nodes,
		// you shouldn't end up with more than you started with
		Assert.assertEquals(result.size(), nodes.size());
		
		// There were a number of ways to do the query in this quiz, reasonably well;
		// we're just doing a heuristics-based optimizer, so, only ignore the really
		// bad case where "hobbies" is the outermost node in the left-deep tree.
		Assert.assertFalse(result.get(0).t1Alias == "hobbies");
		
		// Also check for some of the other silly cases, like forcing a cross join by
		// "hobbies" only being at the two extremes, or "hobbies" being the outermost table.
		Assert.assertFalse(result.get(2).t2Alias == "hobbies" && (result.get(0).t1Alias == "hobbies" || result.get(0).t2Alias == "hobbies"));
	}
	
	/**
	 * Test a much-larger join ordering, to confirm that it executes in a reasonable amount of time
	 */
	@Test(timeout=60000) public void bigOrderJoinsTest() throws IOException, DbException, TransactionAbortedException, ParsingException {
		final int IO_COST = 103;
		
		JoinOptimizer j;
		HashMap<String, TableStats> stats = new HashMap<String,TableStats>();
		Vector<LogicalJoinNode> result;
		Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
		HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();
		TransactionId tid = new TransactionId();
		
		// Create a large set of tables, and add tuples to the tables
		ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100, Integer.MAX_VALUE, null, smallHeapFileTuples, "c");		
		HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		
		ArrayList<ArrayList<Integer>> bigHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < 100000; i++) {
			bigHeapFileTuples.add( smallHeapFileTuples.get( i%100 ) );
		}
		HeapFile bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2, "c");
		Database.getCatalog().addTable(bigHeapFile, "bigTable");

		// Add the tables to the database
		Database.getCatalog().addTable(bigHeapFile, "bigTable");
		Database.getCatalog().addTable(smallHeapFileA, "a");
		Database.getCatalog().addTable(smallHeapFileB, "b");
		Database.getCatalog().addTable(smallHeapFileC, "c");
		Database.getCatalog().addTable(smallHeapFileD, "d");
		Database.getCatalog().addTable(smallHeapFileE, "e");
		Database.getCatalog().addTable(smallHeapFileF, "f");
		Database.getCatalog().addTable(smallHeapFileG, "g");
		Database.getCatalog().addTable(smallHeapFileH, "h");
		Database.getCatalog().addTable(smallHeapFileI, "i");
		Database.getCatalog().addTable(smallHeapFileJ, "j");
		Database.getCatalog().addTable(smallHeapFileK, "k");
		Database.getCatalog().addTable(smallHeapFileL, "l");
		Database.getCatalog().addTable(smallHeapFileM, "m");
		Database.getCatalog().addTable(smallHeapFileN, "n");
		
		// Come up with join statistics for the tables
		stats.put("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
		stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
		stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
		stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
		stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
		stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
		stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
		stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("h", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("i", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("j", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("k", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("l", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("m", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("n", new TableStats(smallHeapFileG.getId(), IO_COST));
		
		// Put in some filter selectivities
		filterSelectivities.put("bigTable", 1.0);
		filterSelectivities.put("a", 1.0);
		filterSelectivities.put("b", 1.0);
		filterSelectivities.put("c", 1.0);
		filterSelectivities.put("d", 1.0);
		filterSelectivities.put("e", 1.0);
		filterSelectivities.put("f", 1.0);
		filterSelectivities.put("g", 1.0);
		filterSelectivities.put("h", 1.0);
		filterSelectivities.put("i", 1.0);
		filterSelectivities.put("j", 1.0);
		filterSelectivities.put("k", 1.0);
		filterSelectivities.put("l", 1.0);
		filterSelectivities.put("m", 1.0);
		filterSelectivities.put("n", 1.0);
		
		// Add the nodes to a collection for a query plan
		nodes.add(new LogicalJoinNode("a", "b", "c1", "c1", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("d", "e", "c0", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("e", "f", "c1", "c1", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("f", "g", "c0", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("g", "h", "c1", "c1", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("h", "i", "c0", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("i", "j", "c1", "c1", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("j", "k", "c0", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("k", "l", "c1", "c1", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("l", "m", "c0", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("m", "n", "c1", "c1", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("n", "bigTable", "c0", "c0", Predicate.Op.EQUALS));
		
		// Make sure we don't give the nodes to the optimizer in a nice order
		Collections.shuffle(nodes);
		Parser p = new Parser();
		j = new JoinOptimizer(
				p.generateLogicalPlan(tid, "SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1;"),
				nodes);
		
		// Set the last boolean here to 'true' in order to have orderJoins() print out its logic
		result = j.orderJoins(stats, filterSelectivities, false);
		
		// If you're only re-ordering the join nodes,
		// you shouldn't end up with more than you started with
		Assert.assertEquals(result.size(), nodes.size());
		
		// Make sure that "bigTable" is the outermost table in the join
		Assert.assertEquals(result.get(result.size()-1).t2Alias, "bigTable");
	}
	
	/**
	 * Test a join ordering with an inequality, to make sure the inequality gets put
	 * as the innermost join
	 */
	@Test public void nonequalityOrderJoinsTest() throws IOException, DbException, TransactionAbortedException, ParsingException {
		final int IO_COST = 103;
		
		JoinOptimizer j;
		HashMap<String, TableStats> stats = new HashMap<String,TableStats>();
		Vector<LogicalJoinNode> result;
		Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
		HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();
		TransactionId tid = new TransactionId();
		
		// Create a large set of tables, and add tuples to the tables
		ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100, Integer.MAX_VALUE, null, smallHeapFileTuples, "c");		
		HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		
		// Add the tables to the database
		Database.getCatalog().addTable(smallHeapFileA, "a");
		Database.getCatalog().addTable(smallHeapFileB, "b");
		Database.getCatalog().addTable(smallHeapFileC, "c");
		Database.getCatalog().addTable(smallHeapFileD, "d");
		
		// Come up with join statistics for the tables
		stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
		stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
		stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
		stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
		
		// Put in some filter selectivities
		filterSelectivities.put("a", 1.0);
		filterSelectivities.put("b", 1.0);
		filterSelectivities.put("c", 1.0);
		filterSelectivities.put("d", 1.0);
		
		// Add the nodes to a collection for a query plan
		nodes.add(new LogicalJoinNode("a", "b", "c1", "c1", Predicate.Op.LESS_THAN));
		nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
		nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));

		Parser p = new Parser();
		// Run the optimizer; see what results we get back
		j = new JoinOptimizer(
				p.generateLogicalPlan(tid, "SELECT COUNT(a.c0) FROM a, b, c, d WHERE a.c1 < b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1;"),
				nodes);
		
		// Set the last boolean here to 'true' in order to have orderJoins() print out its logic
		result = j.orderJoins(stats, filterSelectivities, false);
		
		// If you're only re-ordering the join nodes,
		// you shouldn't end up with more than you started with
		Assert.assertEquals(result.size(), nodes.size());
		
		// Make sure that "a" is the outermost table in the join
		Assert.assertTrue(result.get(result.size() - 1).t2Alias.equals("a") || result.get(result.size() - 1).t1Alias.equals("a"));
	}
}
