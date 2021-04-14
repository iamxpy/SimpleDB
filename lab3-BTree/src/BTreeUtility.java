package simpledb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import simpledb.Predicate.Op;

/** Helper methods used for testing and implementing random features. */
public class BTreeUtility {

	public static final int MAX_RAND_VALUE = 1 << 16;

	public static ArrayList<Integer> tupleToList(Tuple tuple) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < tuple.getTupleDesc().numFields(); ++i) {
            int value = ((IntField)tuple.getField(i)).getValue();
            list.add(value);
        }
        return list;
    }
	
	/**
	 * @return a Tuple with a single IntField with value n and with
	 *   RecordId(BTreePageId(1,2, BTreePageId.LEAF), 3)
	 */
	public static Tuple getBTreeTuple(int n) {
		Tuple tup = new Tuple(Utility.getTupleDesc(1));
		tup.setRecordId(new RecordId(new BTreePageId(1, 2, BTreePageId.LEAF), 3));
		tup.setField(0, new IntField(n));
		return tup;
	}

	/**
	 * @return a Tuple with an IntField for every element of tupdata
	 *   and RecordId(BTreePageId(1, 2, BTreePageId.LEAF), 3)
	 */
	public static Tuple getBTreeTuple(int[] tupdata) {
		Tuple tup = new Tuple(Utility.getTupleDesc(tupdata.length));
		tup.setRecordId(new RecordId(new BTreePageId(1, 2, BTreePageId.LEAF), 3));
		for (int i = 0; i < tupdata.length; ++i)
			tup.setField(i, new IntField(tupdata[i]));
		return tup;
	}
	
	/**
	 * @return a Tuple with an IntField for every element of tupdata
	 *   and RecordId(BTreePageId(1, 2, BTreePageId.LEAF), 3)
	 */
	public static Tuple getBTreeTuple(ArrayList<Integer> tupdata) {
		Tuple tup = new Tuple(Utility.getTupleDesc(tupdata.size()));
		tup.setRecordId(new RecordId(new BTreePageId(1, 2, BTreePageId.LEAF), 3));
		for (int i = 0; i < tupdata.size(); ++i)
			tup.setField(i, new IntField(tupdata.get(i)));
		return tup;
	}

	/**
	 * @return a Tuple with a 'width' IntFields each with value n and
	 *   with RecordId(BTreePageId(1, 2, BTreePageId.LEAF), 3)
	 */
	public static Tuple getBTreeTuple(int n, int width) {
		Tuple tup = new Tuple(Utility.getTupleDesc(width));
		tup.setRecordId(new RecordId(new BTreePageId(1, 2, BTreePageId.LEAF), 3));
		for (int i = 0; i < width; ++i)
			tup.setField(i, new IntField(n));
		return tup;
	}

	/**
	 * @return a BTreeEntry with an IntField with value n and with
	 *   RecordId(BTreePageId(1,2, BTreePageId.INTERNAL), 3)
	 */
	public static BTreeEntry getBTreeEntry(int n) {
		BTreePageId leftChild = new BTreePageId(1, n, BTreePageId.LEAF);
		BTreePageId rightChild = new BTreePageId(1, n+1, BTreePageId.LEAF);
		BTreeEntry e = new BTreeEntry(new IntField(n), leftChild, rightChild);
		e.setRecordId(new RecordId(new BTreePageId(1, 2, BTreePageId.INTERNAL), 3));
		return e;
	}

	/**
	 * @return a BTreeEntry with an IntField with value n and with
	 *   RecordId(BTreePageId(tableid,2, BTreePageId.INTERNAL), 3)
	 */
	public static BTreeEntry getBTreeEntry(int n, int tableid) {
		BTreePageId leftChild = new BTreePageId(tableid, n, BTreePageId.LEAF);
		BTreePageId rightChild = new BTreePageId(tableid, n+1, BTreePageId.LEAF);
		BTreeEntry e = new BTreeEntry(new IntField(n), leftChild, rightChild);
		e.setRecordId(new RecordId(new BTreePageId(tableid, 2, BTreePageId.INTERNAL), 3));
		return e;
	}

	/**
	 * @return a BTreeEntry with an IntField with value key and with
	 *   RecordId(BTreePageId(tableid,2, BTreePageId.INTERNAL), 3)
	 */
	public static BTreeEntry getBTreeEntry(int n, int key, int tableid) {
		BTreePageId leftChild = new BTreePageId(tableid, n, BTreePageId.LEAF);
		BTreePageId rightChild = new BTreePageId(tableid, n+1, BTreePageId.LEAF);
		BTreeEntry e = new BTreeEntry(new IntField(key), leftChild, rightChild);
		e.setRecordId(new RecordId(new BTreePageId(tableid, 2, BTreePageId.INTERNAL), 3));
		return e;
	}

	/** @param columnSpecification Mapping between column index and value. */
	public static BTreeFile createRandomBTreeFile(
			int columns, int rows, Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples, int keyField)
					throws IOException, DbException, TransactionAbortedException {
		return createRandomBTreeFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples, keyField);
	}

	/**
	 * Generates a random B+ tree file for testing
	 * @param columns - number of columns
	 * @param rows - number of rows
	 * @param maxValue - the maximum random value in this B+ tree
	 * @param columnSpecification - optional column specification
	 * @param tuples - optional list of tuples to return
	 * @param keyField - the index of the key field
	 * @return a BTreeFile
	 * @throws IOException
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public static BTreeFile createRandomBTreeFile(int columns, int rows,
			int maxValue, Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples, int keyField) 
					throws IOException, DbException, TransactionAbortedException {

		if (tuples != null) {
			tuples.clear();
		} else {
			tuples = new ArrayList<ArrayList<Integer>>(rows);
		}

		generateRandomTuples(columns, rows, maxValue, columnSpecification, tuples);
		
		// Convert the tuples list to a B+ tree file
		File hFile = File.createTempFile("table", ".dat");
		hFile.deleteOnExit();

		File bFile = File.createTempFile("table_index", ".dat");
		bFile.deleteOnExit();

		Type[] typeAr = new Type[columns];
		Arrays.fill(typeAr, Type.INT_TYPE);
		return BTreeFileEncoder.convert(tuples, hFile, bFile, BufferPool.getPageSize(),
				columns, typeAr, ',', keyField) ;
	}
	
	/**
	 * Generate a random set of tuples for testing
	 * @param columns - number of columns
	 * @param rows - number of rows
	 * @param maxValue - the maximum random value in this B+ tree
	 * @param columnSpecification - optional column specification
	 * @param tuples - list of tuples to return
	 */
	public static void generateRandomTuples(int columns, int rows,
			int maxValue, Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples) {
		generateRandomTuples(columns, rows, 0, maxValue, columnSpecification, tuples);
	}
	
	/**
	 * Generate a random set of tuples for testing
	 * @param columns - number of columns
	 * @param rows - number of rows
	 * @param minValue - the minimum random value in this B+ tree
	 * @param maxValue - the maximum random value in this B+ tree
	 * @param columnSpecification - optional column specification
	 * @param tuples - list of tuples to return
	 */
	public static void generateRandomTuples(int columns, int rows,
			int minValue, int maxValue, Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples) {

		Random r = new Random();

		// Fill the tuples list with generated values
		for (int i = 0; i < rows; ++i) {
			ArrayList<Integer> tuple = new ArrayList<Integer>(columns);
			for (int j = 0; j < columns; ++j) {
				// Generate random values, or use the column specification
				Integer columnValue = null;
				if (columnSpecification != null) columnValue = columnSpecification.get(j);
				if (columnValue == null) {
					columnValue = r.nextInt(maxValue-minValue) + minValue;
				}
				tuple.add(columnValue);
			}
			tuples.add(tuple);
		}
	}
	
	/**
	 * Generate a random set of entries for testing
	 * @param numKeys - number of keys
	 * @param minKey - the minimum key value
	 * @param maxKey - the maximum key value
	 * @param minChildPtr - the first child pointer
	 * @param childPointers - list of child pointers to return
	 * @param keys - list of keys to return
	 */
	public static void generateRandomEntries(int numKeys, int minKey, int maxKey, int minChildPtr,
			ArrayList<Integer> childPointers, ArrayList<Integer> keys) {

		Random r = new Random();

		// Fill the keys and childPointers lists with generated values
		int child = minChildPtr;
		for (int i = 0; i < numKeys; ++i) {
			keys.add(r.nextInt(maxKey-minKey) + minKey);
			childPointers.add(child);
			++child;
		}
		
		// one extra child pointer
		childPointers.add(child);
	}
	
	/**
	 * Generate a random set of tuples for testing
	 * @param columns - number of columns
	 * @param rows - number of rows
	 * @param min - the minimum value
	 * @param max - the maximum value
	 * @return the list of tuples
	 */
	public static ArrayList<Tuple> generateRandomTuples(int columns, int rows, int min, int max) {
		ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>(rows);
		generateRandomTuples(columns, rows, min, max, null, tuples);
		ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
		for(ArrayList<Integer> tup : tuples) {
			tupleList.add(getBTreeTuple(tup));	
		}
		return tupleList;
	}
	
	/**
	 * Generate a random set of entries for testing
	 * @param numKeys - the number of keys
	 * @param tableid - the tableid
	 * @param childPageCategory - the child page category (LEAF or INTERNAL)
	 * @param minKey - the minimum key value
	 * @param maxKey - the maximum key value
	 * @param minChildPtr - the first child pointer
	 * @return the list of entries
	 */
	public static ArrayList<BTreeEntry> generateRandomEntries(int numKeys, int tableid, int childPageCategory, int minKey, int maxKey, int minChildPtr) {
		ArrayList<Integer> keys = new ArrayList<Integer>(numKeys);
		ArrayList<Integer> childPointers = new ArrayList<Integer>(numKeys+1);
		generateRandomEntries(numKeys, minKey, maxKey, minChildPtr, childPointers, keys);
		Collections.sort(keys);
		ArrayList<BTreeEntry> entryList = new ArrayList<BTreeEntry>();
		for(int i = 0; i < numKeys; ++i) {
			entryList.add(new BTreeEntry(new IntField(keys.get(i)), 
					new BTreePageId(tableid, childPointers.get(i), childPageCategory), 
					new BTreePageId(tableid, childPointers.get(i+1), childPageCategory)));
		}
		return entryList;
	}
	
	/**
	 * Get the number of tuples that can fit on a page with the specified number of integer fields
	 * @param columns - the number of columns
	 * @return the number of tuples per page
	 */
	public static int getNumTuplesPerPage(int columns) {
		int bytesPerTuple = Type.INT_TYPE.getLen() * columns * 8;
		int tuplesPerPage = (BufferPool.getPageSize() * 8 - 3 * BTreeLeafPage.INDEX_SIZE * 8) /  (bytesPerTuple + 1);
		return tuplesPerPage;
	}
	
	/**
	 * Create a random leaf page for testing
	 * @param pid - the page id of the leaf page
	 * @param columns - the number of fields per tuple
	 * @param keyField - the index of the key field in each tuple
	 * @param min - the minimum value
	 * @param max - the maximum value
	 * @return the leaf page
	 * @throws IOException
	 */
	public static BTreeLeafPage createRandomLeafPage(BTreePageId pid, int columns, int keyField, int min, int max) throws IOException {
		int tuplesPerPage = getNumTuplesPerPage(columns);
		return createRandomLeafPage(pid, columns, keyField, tuplesPerPage, min, max);
	}
	
	/**
	 * Create a random leaf page for testing
	 * @param pid - the page id of the leaf page
	 * @param columns - the number of fields per tuple
	 * @param keyField - the index of the key field in each tuple
	 * @param numTuples - the number of tuples to insert
	 * @param min - the minimum value
	 * @param max - the maximum value
	 * @return the leaf page
	 * @throws IOException
	 */
	public static BTreeLeafPage createRandomLeafPage(BTreePageId pid, int columns, int keyField, int numTuples, int min, int max) throws IOException {
		Type[] typeAr = new Type[columns];
		Arrays.fill(typeAr, Type.INT_TYPE);
		byte[] data = BTreeFileEncoder.convertToLeafPage(BTreeUtility.generateRandomTuples(columns, numTuples, min, max), 
				BufferPool.getPageSize(), columns, typeAr, keyField);
		BTreeLeafPage page = new BTreeLeafPage(pid, data, keyField);
		return page;
	}

	/**
	 * The number of entries that can fit on a page with integer key fields
	 * @return the number of entries per page
	 */
	public static int getNumEntriesPerPage() {
		int nentrybytes = Type.INT_TYPE.getLen() + BTreeInternalPage.INDEX_SIZE;
		// pointerbytes: one extra child pointer, parent pointer, child page category
		int internalpointerbytes = 2 * BTreeLeafPage.INDEX_SIZE + 1; 
		int entriesPerPage = (BufferPool.getPageSize() * 8 - internalpointerbytes * 8 - 1) /  (nentrybytes * 8 + 1);  //floor comes for free
		return entriesPerPage;
	}
	
	/**
	 * Create a random internal page for testing
	 * @param pid - the page id of the internal page
	 * @param keyField - the index of the key field in each tuple
	 * @param childPageCategory - the child page category (LEAF or INTERNAL)
	 * @param minKey - the minimum key value
	 * @param maxKey - the maximum key value
	 * @param minChildPtr - the first child pointer
	 * @return the internal page
	 * @throws IOException
	 */
	public static BTreeInternalPage createRandomInternalPage(BTreePageId pid, int keyField, int childPageCategory, int minKey, int maxKey, int minChildPtr) throws IOException {
		int entriesPerPage = getNumEntriesPerPage();
		return createRandomInternalPage(pid, keyField, childPageCategory, entriesPerPage, minKey, maxKey, minChildPtr);
	}
	
	/**
	 * Create a random internal page for testing
	 * @param pid - the page id of the internal page
	 * @param keyField - the index of the key field in each tuple
	 * @param childPageCategory - the child page category (LEAF or INTERNAL)
	 * @param numKeys - the number of keys to insert
	 * @param minKey - the minimum key value
	 * @param maxKey - the maximum key value
	 * @param minChildPtr - the first child pointer
	 * @return the internal page
	 * @throws IOException
	 */
	public static BTreeInternalPage createRandomInternalPage(BTreePageId pid, int keyField, int childPageCategory, int numKeys, int minKey, int maxKey, int minChildPtr) throws IOException {
		byte[] data = BTreeFileEncoder.convertToInternalPage(BTreeUtility.generateRandomEntries(numKeys, pid.getTableId(), childPageCategory, minKey, maxKey, minChildPtr), 
				BufferPool.getPageSize(), Type.INT_TYPE, childPageCategory);
		BTreeInternalPage page = new BTreeInternalPage(pid, data, keyField);
		return page;
	}

	/**
	 * creates a *non* random B+ tree file for testing
	 * @param columns - number of columns
	 * @param rows - number of rows
	 * @param columnSpecification - optional column specification
	 * @param tuples - optional list of tuples to return
	 * @param keyField - the index of the key field
	 * @return a BTreeFile
	 * @throws IOException
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public static BTreeFile createBTreeFile(int columns, int rows,
			Map<Integer, Integer> columnSpecification,
			ArrayList<ArrayList<Integer>> tuples, int keyField) 
					throws IOException, DbException, TransactionAbortedException {
		if (tuples != null) {
			tuples.clear();
		} else {
			tuples = new ArrayList<ArrayList<Integer>>(rows);
		}

		// Fill the tuples list with generated values
		for (int i = 0; i < rows; ++i) {
			ArrayList<Integer> tuple = new ArrayList<Integer>(columns);
			for (int j = 0; j < columns; ++j) {
				// Generate values, or use the column specification
				Integer columnValue = null;
				if (columnSpecification != null) columnValue = columnSpecification.get(j);
				if (columnValue == null) {
					columnValue = (i+1)*(j+1);
				}
				tuple.add(columnValue);
			}
			tuples.add(tuple);
		}

		// Convert the tuples list to a B+ tree file
		File hFile = File.createTempFile("table", ".dat");
		hFile.deleteOnExit();

		File bFile = File.createTempFile("table_index", ".dat");
		bFile.deleteOnExit();

		Type[] typeAr = new Type[columns];
		Arrays.fill(typeAr, Type.INT_TYPE);
		return BTreeFileEncoder.convert(tuples, hFile, bFile, BufferPool.getPageSize(),
				columns, typeAr, ',', keyField) ;
	}

	/** Opens a BTreeFile and adds it to the catalog.
	 *
	 * @param cols number of columns in the table.
	 * @param f location of the file storing the table.
	 * @param keyField the field the B+ tree is keyed on
	 * @return the opened table.
	 */
	public static BTreeFile openBTreeFile(int cols, File f, int keyField) {
		// create the BTreeFile and add it to the catalog
		TupleDesc td = Utility.getTupleDesc(cols);
		BTreeFile bf = new BTreeFile(f, keyField, td);
		Database.getCatalog().addTable(bf, UUID.randomUUID().toString());
		return bf;
	}

	public static BTreeFile openBTreeFile(int cols, String colPrefix, File f, int keyField) {
		// create the BTreeFile and add it to the catalog
		TupleDesc td = Utility.getTupleDesc(cols, colPrefix);
		BTreeFile bf = new BTreeFile(f, keyField, td);
		Database.getCatalog().addTable(bf, UUID.randomUUID().toString());
		return bf;
	}

	/**
	 * A utility method to create a new BTreeFile with no data,
	 * assuming the path does not already exist. If the path exists, the file
	 * will be overwritten. The new table will be added to the Catalog with
	 * the specified number of columns as IntFields indexed on the keyField.
	 */
	public static BTreeFile createEmptyBTreeFile(String path, int cols, int keyField)
			throws IOException {
		File f = new File(path);
		// touch the file
		FileOutputStream fos = new FileOutputStream(f);
		fos.write(new byte[0]);
		fos.close();

		BTreeFile bf = openBTreeFile(cols, f, keyField);

		return bf;
	}

	/**
	 * A utility method to create a new BTreeFile with no data, with the specified
	 * number of pages, assuming the path does not already exist. If the path exists, 
	 * the file will be overwritten. The new table will be added to the Catalog with
	 * the specified number of columns as IntFields indexed on the keyField.
	 */
	public static BTreeFile createEmptyBTreeFile(String path, int cols, int keyField, int pages)
			throws IOException {
		File f = new File(path);
		BufferedOutputStream bw = new BufferedOutputStream(
				new FileOutputStream(f, true));
		byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
		byte[] emptyPageData = BTreePage.createEmptyPageData();
		bw.write(emptyRootPtrData);
		for(int i = 0; i < pages; ++i) {
			bw.write(emptyPageData);
		}
		bw.close();

		BTreeFile bf = openBTreeFile(cols, f, keyField);

		return bf;
	}

	/**
	 * Helper class that attempts to insert a tuple in a new thread
	 *
	 * @return a handle to the Thread that will attempt insertion after it
	 *   has been started
	 */
	static class BTreeWriter extends Thread {

		TransactionId tid;
		BTreeFile bf;
		int item;
		int count;
		boolean success;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param tid the transaction on whose behalf we want to insert the tuple
		 * @param bf the B+ tree file into which we want to insert the tuple
		 * @param item the key of the tuple to insert
		 * @param count the number of times to insert the tuple
		 */
		public BTreeWriter(TransactionId tid, BTreeFile bf, int item, int count) {
			this.tid = tid;
			this.bf = bf;
			this.item = item;
			this.count = count;
			this.success = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}

		public void run() {
			try {
				int c = 0;
				while(c < count) {
					Tuple t = BTreeUtility.getBTreeTuple(item, 2);
					Database.getBufferPool().insertTuple(tid, bf.getId(), t);

					IndexPredicate ipred = new IndexPredicate(Op.EQUALS, t.getField(bf.keyField()));
					DbFileIterator it = bf.indexIterator(tid, ipred);
					it.open();
					c = 0;
					while(it.hasNext()) {
						it.next();
						c++;
					}
					it.close();
				}
				synchronized(slock) {
					success = true;
				}
			} catch (Exception e) {
				e.printStackTrace();
				synchronized(elock) {
					error = e;
				}

				try {
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				}
			}
		}

		/**
		 * @return true if we successfully inserted the tuple
		 */
		 public boolean succeeded() {
			 synchronized(slock) {
				 return success;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while inserting the tuple;
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}

	/**
	 * Helper class that searches for tuple(s) in a new thread
	 *
	 * @return a handle to the Thread that will attempt to search for tuple(s) after it
	 *   has been started
	 */
	static class BTreeReader extends Thread {

		TransactionId tid;
		BTreeFile bf;
		Field f;
		int count;
		boolean found;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param tid the transaction on whose behalf we want to search for the tuple(s)
		 * @param bf the B+ tree file containing the tuple(s)
		 * @param f the field to search for
		 * @param count the number of tuples to search for
		 */
		public BTreeReader(TransactionId tid, BTreeFile bf, Field f, int count) {
			this.tid = tid;
			this.bf = bf;
			this.f = f;
			this.count = count;
			this.found = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}

		public void run() {
			try {
				while(true) {
					IndexPredicate ipred = new IndexPredicate(Op.EQUALS, f);
					DbFileIterator it = bf.indexIterator(tid, ipred);
					it.open();
					int c = 0;
					while(it.hasNext()) {
						it.next();
						c++;
					}
					it.close();
					if(c >= count) {
						synchronized(slock) {
							found = true;
						}
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
				synchronized(elock) {
					error = e;
				}

				try {
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				}
			}
		}

		/**
		 * @return true if we successfully found the tuple(s)
		 */
		 public boolean found() {
			 synchronized(slock) {
				 return found;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while searching for the tuple(s);
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}
	
	/**
	 * Helper class that attempts to insert a tuple in a new thread
	 *
	 * @return a handle to the Thread that will attempt insertion after it
	 *   has been started
	 */
	public static class BTreeInserter extends Thread {

		TransactionId tid;
		BTreeFile bf;
		int[] tupdata;
		BlockingQueue<ArrayList<Integer>> insertedTuples;
		boolean success;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param bf the B+ tree file into which we want to insert the tuple
		 * @param tupdata the data of the tuple to insert
		 * @param the list of tuples that were successfully inserted
		 */
		public BTreeInserter(BTreeFile bf, int[] tupdata, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, tupdata, insertedTuples);
		}

		public void run() {
			try {
				Tuple t = BTreeUtility.getBTreeTuple(tupdata);
				Database.getBufferPool().insertTuple(tid, bf.getId(), t);
				Database.getBufferPool().transactionComplete(tid);
				ArrayList<Integer> tuple = tupleToList(t);
				insertedTuples.put(tuple);
				synchronized(slock) {
					success = true;
				}
			} catch (Exception e) {
				if(!(e instanceof TransactionAbortedException)) {
					e.printStackTrace();
				}
				synchronized(elock) {
					error = e;
				}

				try {
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				}
			}
		}
		
		private void init(BTreeFile bf, int[] tupdata, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			this.tid = new TransactionId();
			this.bf = bf;
			this.tupdata = tupdata;
			this.insertedTuples = insertedTuples;
			this.success = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}
		
		public void rerun(BTreeFile bf, int[] tupdata, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, tupdata, insertedTuples);
			run();
		}

		/**
		 * @return true if we successfully inserted the tuple
		 */
		 public boolean succeeded() {
			 synchronized(slock) {
				 return success;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while inserting the tuple;
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}
    
	/**
	 * Helper class that attempts to delete tuple(s) in a new thread
	 *
	 * @return a handle to the Thread that will attempt deletion after it
	 *   has been started
	 */
	public static class BTreeDeleter extends Thread {

		TransactionId tid;
		BTreeFile bf;
		BlockingQueue<ArrayList<Integer>> insertedTuples;
		ArrayList<Integer> tuple;
		boolean success;
		Exception error;
		Object slock;
		Object elock;

		/**
		 * @param bf the B+ tree file from which we want to delete the tuple(s)
		 * @param the list of tuples to delete
		 */
		public BTreeDeleter(BTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, insertedTuples);
		}

		public void run() {
			try {
				tuple = insertedTuples.take();
				if(bf.getTupleDesc().numFields() != tuple.size()) {
					throw new DbException("tuple desc mismatch");
				}
				IntField key = new IntField(tuple.get(bf.keyField()));
				IndexPredicate ipred = new IndexPredicate(Op.EQUALS, key);
				DbFileIterator it = bf.indexIterator(tid, ipred);
				it.open();
				while(it.hasNext()) {
					Tuple t = it.next();
					if(tupleToList(t).equals(tuple)) {
						Database.getBufferPool().deleteTuple(tid, t);
						break;
					}
				}
				it.close();
				Database.getBufferPool().transactionComplete(tid);
				synchronized(slock) {
					success = true;
				}
			} catch (Exception e) {
				if(!(e instanceof TransactionAbortedException)) {
					e.printStackTrace();
				}
				synchronized(elock) {
					error = e;
				}

				try {
					insertedTuples.put(tuple);
					Database.getBufferPool().transactionComplete(tid, false);
				} catch (java.io.IOException e2) {
					e2.printStackTrace();
				} catch (InterruptedException e3) {
					e3.printStackTrace();
				}
			}
		}
		
		private void init(BTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			this.tid = new TransactionId();
			this.bf = bf;
			this.insertedTuples = insertedTuples;
			this.success = false;
			this.error = null;
			this.slock = new Object();
			this.elock = new Object();
		}
		
		public void rerun(BTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {
			init(bf, insertedTuples);
			run();
		}

		/**
		 * @return true if we successfully inserted the tuple
		 */
		 public boolean succeeded() {
			 synchronized(slock) {
				 return success;
			 }
		 }

		/**
		 * @return an Exception instance if one occurred while inserting the tuple;
		 *   null otherwise
		 */
		 public Exception getError() {
			 synchronized(elock) {
				 return error;
			 }
		 }
	}

}

