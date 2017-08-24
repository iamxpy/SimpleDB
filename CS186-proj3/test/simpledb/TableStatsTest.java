package simpledb;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class TableStatsTest extends SimpleDbTestBase {
	public static final int IO_COST = 71;
	
	ArrayList<ArrayList<Integer>> tuples;
	HeapFile f;
	String tableName;
	int tableId;
	
	@Before public void setUp() throws Exception {
		super.setUp();
		this.tuples = new ArrayList<ArrayList<Integer>>();
		this.f = SystemTestUtil.createRandomHeapFile(10, 1020, 32, null, tuples);
		
		this.tableName = SystemTestUtil.getUUID();
		Database.getCatalog().addTable(f, tableName);
		this.tableId = Database.getCatalog().getTableId(tableName);		
	}
	
	private double[] getRandomTableScanCosts(int[] pageNums, int[] ioCosts) throws IOException, DbException, TransactionAbortedException {
		double[] ret = new double[ioCosts.length];
		for(int i = 0; i < ioCosts.length; ++i) {
			HeapFile hf = SystemTestUtil.createRandomHeapFile(1, 992*pageNums[i], 32, null, tuples);
			Assert.assertEquals(pageNums[i], hf.numPages());			
			String tableName = SystemTestUtil.getUUID();
			Database.getCatalog().addTable(hf, tableName);
			int tableId = Database.getCatalog().getTableId(tableName);
			ret[i] = (new TableStats(tableId, ioCosts[i])).estimateScanCost();
		}
		return ret;
	}
	/**
	 * Verify the cost estimates of scanning various numbers of pages from a HeapFile
	 * This test checks that the estimateScanCost is: 
	 *   +linear in numPages when IO_COST is constant
	 *   +linear in IO_COST when numPages is constant
	 *   +quadratic when IO_COST and numPages increase linearly.
	 */
	@Test public void estimateScanCostTest() throws IOException, DbException, TransactionAbortedException {
		Object[] ret;
		int[] ioCosts = new int[20];
		int[] pageNums = new int[ioCosts.length];
		// IO_COST constant, numPages change
		for(int i = 0; i < ioCosts.length; ++i) {
			ioCosts[i] = 1;
			pageNums[i] = 3*(i+1);
		}
		double stats[] = getRandomTableScanCosts(pageNums, ioCosts);
		ret = SystemTestUtil.checkConstant(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals(ret[0], Boolean.TRUE);
		// numPages constant, IO_COST change
		for(int i = 0; i < ioCosts.length; ++i) {
			ioCosts[i] = 10*(i + 1);
			pageNums[i] = 3;
		}
		stats = getRandomTableScanCosts(pageNums, ioCosts);
		ret = SystemTestUtil.checkConstant(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals(ret[0], Boolean.TRUE);
		//numPages & IO_COST increase linearly
		for(int i = 0; i < ioCosts.length; ++i) {
			ioCosts[i] = 3*(i + 1);
			pageNums[i] = (i+1);
		}
		stats = getRandomTableScanCosts(pageNums, ioCosts);
		ret = SystemTestUtil.checkConstant(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkLinear(stats);
		Assert.assertEquals(ret[0], Boolean.FALSE);
		ret = SystemTestUtil.checkQuadratic(stats);
		Assert.assertEquals(ret[0], Boolean.TRUE);
		
	}
	
	/**
	 * Verify the table-cardinality estimates based on a selectivity estimate
	 */
	@Test public void estimateTableCardinalityTest() {
		TableStats s = new TableStats(this.tableId, IO_COST);
		
		// Try a random selectivity
		Assert.assertEquals(306, s.estimateTableCardinality(0.3));
		
		// Make sure we get all rows with 100% selectivity, and none with 0%
		Assert.assertEquals(1020, s.estimateTableCardinality(1.0));
		Assert.assertEquals(0, s.estimateTableCardinality(0.0));
	}
	
	/**
	 * Verify that selectivity estimates do something reasonable.
	 * Don't bother splitting this into N different functions for
	 * each possible Op because we will probably catch any bugs here in
	 * IntHistogramTest, so we hopefully don't need all the JUnit checkboxes.
	 */
	@Test public void estimateSelectivityTest() {
		final int maxCellVal = 32;	// Tuple values are randomized between 0 and this number
		
		final Field aboveMax = new IntField(maxCellVal + 10);
		final Field atMax = new IntField(maxCellVal);
		final Field halfMaxMin = new IntField(maxCellVal/2);
		final Field atMin = new IntField(0);
		final Field belowMin = new IntField(-10);
		
		TableStats s = new TableStats(this.tableId, IO_COST);
		
		for (int col = 0; col < 10; col++) {
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.EQUALS, aboveMax), 0.001);			
			Assert.assertEquals(1.0/32.0, s.estimateSelectivity(col, Predicate.Op.EQUALS, halfMaxMin), 0.015);
			Assert.assertEquals(0, s.estimateSelectivity(col, Predicate.Op.EQUALS, belowMin), 0.001);

			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.NOT_EQUALS, aboveMax), 0.001);
			Assert.assertEquals(31.0/32.0, s.estimateSelectivity(col, Predicate.Op.NOT_EQUALS, halfMaxMin), 0.015);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.NOT_EQUALS, belowMin), 0.015);

			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, aboveMax), 0.001);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, atMax), 0.001);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, halfMaxMin), 0.1);
			Assert.assertEquals(31.0/32.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, atMin), 0.05);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN, belowMin), 0.001);
			
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, aboveMax), 0.001);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, atMax), 0.015);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, halfMaxMin), 0.1);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, atMin), 0.001);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN, belowMin), 0.001);
			
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, aboveMax), 0.001);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, atMax), 0.015);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, halfMaxMin), 0.1);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, atMin), 0.015);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.GREATER_THAN_OR_EQ, belowMin), 0.001);
			
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, aboveMax), 0.001);
			Assert.assertEquals(1.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, atMax), 0.015);
			Assert.assertEquals(0.5, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, halfMaxMin), 0.1);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, atMin), 0.05);
			Assert.assertEquals(0.0, s.estimateSelectivity(col, Predicate.Op.LESS_THAN_OR_EQ, belowMin), 0.001);
		}
	}
}
