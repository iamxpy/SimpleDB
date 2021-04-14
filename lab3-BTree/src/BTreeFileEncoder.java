package simpledb;

import java.io.*;
import java.util.*;

import simpledb.Predicate.Op;

/**
 * BTreeFileEncoder reads a comma delimited text file and converts it to
 * pages of binary data in the appropriate format for simpledb B+ tree
 * pages.
 */

public class BTreeFileEncoder {

	/**
	 * Encode the file using the BTreeFile's Insert method.
	 * 
	 * @param tuples - list of tuples to add to the file
	 * @param hFile - the file to temporarily store the data as a heap file on disk
	 * @param bFile - the file on disk to back the resulting BTreeFile
	 * @param keyField - the index of the key field for this B+ tree
	 * @param numFields - the number of fields in each tuple
	 * @return the BTreeFile
	 */
	public static BTreeFile convert(ArrayList<ArrayList<Integer>> tuples, File hFile, 
			File bFile, int keyField, int numFields) throws IOException {
		File tempInput = File.createTempFile("tempTable", ".txt");
		tempInput.deleteOnExit();
		BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
		for (ArrayList<Integer> tuple : tuples) {
			int writtenFields = 0;
			for (Integer field : tuple) {
				writtenFields++;
				if (writtenFields > numFields) {
					bw.close();
					throw new RuntimeException("Tuple has more than " + numFields + " fields: (" +
							Utility.listToString(tuple) + ")");
				}
				bw.write(String.valueOf(field));
				if (writtenFields < numFields) {
					bw.write(',');
				}
			}
			bw.write('\n');
		}
		bw.close();
		return convert(tempInput, hFile, bFile, keyField, numFields);
	}

	/**
	 * Encode the file using the BTreeFile's Insert method.
	 * 
	 * @param inFile - the raw text file containing the tuples
	 * @param hFile - the file to temporarily store the data as a heap file on disk
	 * @param bFile - the file on disk to back the resulting BTreeFile
	 * @param keyField - the index of the key field for this B+ tree
	 * @param numFields - the number of fields in each tuple
	 * @return the BTreeFile
	 */
	public static BTreeFile convert(File inFile, File hFile, File bFile,
			int keyField, int numFields)
					throws IOException {
		// convert the inFile to HeapFile first.
		HeapFileEncoder.convert(inFile, hFile, BufferPool.getPageSize(), numFields);
		HeapFile heapf = Utility.openHeapFile(numFields, hFile);

		// add the heap file to B+ tree file
		BTreeFile bf = BTreeUtility.openBTreeFile(numFields, bFile, keyField);

		try {
			TransactionId tid = new TransactionId();
			DbFileIterator it = Database.getCatalog().getDatabaseFile(heapf.getId()).iterator(tid);
			it.open();
			int count = 0;
			Transaction t = new Transaction();
			while (it.hasNext()) {
				Tuple tup = it.next();
				Database.getBufferPool().insertTuple(t.getId(), bf.getId(), tup);
				count++;
				if(count >= 40) {
					Database.getBufferPool().flushAllPages();
					count = 0;
				}
				t.commit();
				t = new Transaction();
			}
			it.close();
		} catch(TransactionAbortedException te){
			te.printStackTrace();
			return bf;
		} catch(DbException e) {
			e.printStackTrace();
			return bf;
		} catch(IOException e) {
			e.printStackTrace();
			return bf;
		}

		try {
			Database.getBufferPool().flushAllPages();
		} catch(Exception e) {
			e.printStackTrace();
		}

		return bf;

	}

	/** 
	 * comparator to sort Tuples by key field
	 */
	public static class TupleComparator implements Comparator<Tuple> {
		private int keyField;

		/** 
		 * Construct a TupleComparator
		 * 
		 * @param keyField - the index of the field the tuples are keyed on
		 */
		public TupleComparator(int keyField) {
			this.keyField = keyField;
		}

		/**
		 * Compare two tuples based on their key field
		 * 
		 * @return -1 if t1 < t2, 1 if t1 > t2, 0 if t1 == t2
		 */
		public int compare(Tuple t1, Tuple t2) {
			int cmp = 0;
			if(t1.getField(keyField).compare(Op.LESS_THAN, t2.getField(keyField))) {
				cmp = -1;
			}
			else if(t1.getField(keyField).compare(Op.GREATER_THAN, t2.getField(keyField))) {
				cmp = 1;
			}
			return cmp;
		}
	}

	/**
	 * Faster method to encode the B+ tree file
	 * 
	 * @param tuples - list of tuples to add to the file
	 * @param hFile - the file to temporarily store the data as a heap file on disk
	 * @param bFile - the file on disk to back the resulting BTreeFile
	 * @param npagebytes - number of bytes per page
	 * @param numFields - number of fields per tuple
	 * @param typeAr - array containing the types of the tuples
	 * @param fieldSeparator - character separating fields in the raw data file
	 * @param keyField - the field of the tuples the B+ tree will be keyed on
	 * @return the BTreeFile
	 */
	public static BTreeFile convert(ArrayList<ArrayList<Integer>> tuples, File hFile, 
			File bFile, int npagebytes,
			int numFields, Type[] typeAr, char fieldSeparator, int keyField) 
					throws IOException, DbException, TransactionAbortedException {
		File tempInput = File.createTempFile("tempTable", ".txt");
		tempInput.deleteOnExit();
		BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
		for (ArrayList<Integer> tuple : tuples) {
			int writtenFields = 0;
			for (Integer field : tuple) {
				writtenFields++;
				if (writtenFields > numFields) {
					bw.close();
					throw new RuntimeException("Tuple has more than " + numFields + " fields: (" +
							Utility.listToString(tuple) + ")");
				}
				bw.write(String.valueOf(field));
				if (writtenFields < numFields) {
					bw.write(',');
				}
			}
			bw.write('\n');
		}
		bw.close();
		return convert(tempInput, hFile, bFile, npagebytes,
				numFields, typeAr, fieldSeparator, keyField);
	}

	/** 
	 * Faster method to encode the B+ tree file
	 * 
	 * @param inFile - the file containing the raw data
	 * @param hFile - the data file for the HeapFile to be used as an intermediate conversion step
	 * @param bFile - the data file for the BTreeFile
	 * @param npagebytes - number of bytes per page
	 * @param numFields - number of fields per tuple
	 * @param typeAr - array containing the types of the tuples
	 * @param fieldSeparator - character separating fields in the raw data file
	 * @param keyField - the field of the tuples the B+ tree will be keyed on
	 * @return the B+ tree file
	 * @throws IOException
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public static BTreeFile convert(File inFile, File hFile, File bFile, int npagebytes,
			int numFields, Type[] typeAr, char fieldSeparator, int keyField) 
					throws IOException, DbException, TransactionAbortedException {
		// convert the inFile to HeapFile first.
		HeapFileEncoder.convert(inFile, hFile, BufferPool.getPageSize(), numFields);
		HeapFile heapf = Utility.openHeapFile(numFields, hFile);

		// read all the tuples from the heap file and sort them on the keyField
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		TransactionId tid = new TransactionId();
		DbFileIterator it = Database.getCatalog().getDatabaseFile(heapf.getId()).iterator(tid);
		it.open();
		while (it.hasNext()) {
			Tuple tup = it.next();
			tuples.add(tup);
		}
		it.close();
		Collections.sort(tuples, new TupleComparator(keyField));

		// add the tuples to B+ tree file
		BTreeFile bf = BTreeUtility.openBTreeFile(numFields, bFile, keyField);
		Type keyType = typeAr[keyField];
		int tableid = bf.getId();

		int nrecbytes = 0;
		for (int i = 0; i < numFields ; i++) {
			nrecbytes += typeAr[i].getLen();
		}
		// pointerbytes: left sibling pointer, right sibling pointer, parent pointer
		int leafpointerbytes = 3 * BTreeLeafPage.INDEX_SIZE; 
		int nrecords = (npagebytes * 8 - leafpointerbytes * 8) /  (nrecbytes * 8 + 1);  //floor comes for free

		int nentrybytes = keyType.getLen() + BTreeInternalPage.INDEX_SIZE;
		// pointerbytes: one extra child pointer, parent pointer, child page category
		int internalpointerbytes = 2 * BTreeLeafPage.INDEX_SIZE + 1; 
		int nentries = (npagebytes * 8 - internalpointerbytes * 8 - 1) /  (nentrybytes * 8 + 1);  //floor comes for free

		ArrayList<ArrayList<BTreeEntry>> entries = new ArrayList<ArrayList<BTreeEntry>>();

		// first add some bytes for the root pointer page
		bf.writePage(new BTreeRootPtrPage(BTreeRootPtrPage.getId(tableid), 
				BTreeRootPtrPage.createEmptyPageData()));

		// next iterate through all the tuples and write out leaf pages
		// and internal pages as they fill up.
		// We wait until we have two full pages of tuples before writing out the first page
		// so that we will not end up with any pages containing less than nrecords/2 tuples
		// (unless it's the only page)
		ArrayList<Tuple> page1 = new ArrayList<Tuple>();
		ArrayList<Tuple> page2 = new ArrayList<Tuple>();
		BTreePageId leftSiblingId = null;
		for(Tuple tup : tuples) {
			if(page1.size() < nrecords) {
				page1.add(tup);
			}
			else if(page2.size() < nrecords) {
				page2.add(tup);
			}
			else {
				// write out a page of records
				byte[] leafPageBytes = convertToLeafPage(page1, npagebytes, numFields, typeAr, keyField);
				BTreePageId leafPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.LEAF);
				BTreeLeafPage leafPage = new BTreeLeafPage(leafPid, leafPageBytes, keyField);
				leafPage.setLeftSiblingId(leftSiblingId);
				bf.writePage(leafPage);
				leftSiblingId = leafPid;

				// update the parent by "copying up" the next key
				BTreeEntry copyUpEntry = new BTreeEntry(page2.get(0).getField(keyField), leafPid, null);
				updateEntries(entries, bf, copyUpEntry, 0, nentries, npagebytes, 
						keyType, tableid, keyField);

				page1 = page2;
				page2 = new ArrayList<Tuple>();
				page2.add(tup);
			}
		}

		// now we need to deal with the end cases. There are two options:
		// 1. We have less than or equal to a full page of records. Because of the way the code
		//    was written above, we know this must be the only page
		// 2. We have somewhere between one and two pages of records remaining.
		// For case (1), we write out the page 
		// For case (2), we divide the remaining records equally between the last two pages,
		// write them out, and update the parent's child pointers.
		BTreePageId lastPid = null;
		if(page2.size() == 0) {
			// write out a page of records - this is the root page
			byte[] lastPageBytes = convertToLeafPage(page1, npagebytes, numFields, typeAr, keyField);
			lastPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.LEAF);
			BTreeLeafPage lastPage = new BTreeLeafPage(lastPid, lastPageBytes, keyField);
			lastPage.setLeftSiblingId(leftSiblingId);
			bf.writePage(lastPage);
		}
		else {
			// split the remaining tuples in half
			int remainingTuples = page1.size() + page2.size();
			ArrayList<Tuple> secondToLastPg = new ArrayList<Tuple>();
			ArrayList<Tuple> lastPg = new ArrayList<Tuple>();
			secondToLastPg.addAll(page1.subList(0, remainingTuples/2));
			lastPg.addAll(page1.subList(remainingTuples/2, page1.size()));
			lastPg.addAll(page2);

			// write out the last two pages of records
			byte[] secondToLastPageBytes = convertToLeafPage(secondToLastPg, npagebytes, numFields, typeAr, keyField);
			BTreePageId secondToLastPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.LEAF);
			BTreeLeafPage secondToLastPage = new BTreeLeafPage(secondToLastPid, secondToLastPageBytes, keyField);
			secondToLastPage.setLeftSiblingId(leftSiblingId);
			bf.writePage(secondToLastPage);

			byte[] lastPageBytes = convertToLeafPage(lastPg, npagebytes, numFields, typeAr, keyField);
			lastPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.LEAF);
			BTreeLeafPage lastPage = new BTreeLeafPage(lastPid, lastPageBytes, keyField);
			lastPage.setLeftSiblingId(secondToLastPid);
			bf.writePage(lastPage);

			// update the parent by "copying up" the next key
			BTreeEntry copyUpEntry = new BTreeEntry(lastPg.get(0).getField(keyField), secondToLastPid, lastPid);
			updateEntries(entries, bf, copyUpEntry, 0, nentries, npagebytes, 
					keyType, tableid, keyField);
		}

		// Write out the remaining internal pages
		cleanUpEntries(entries, bf, nentries, npagebytes, keyType, tableid, keyField);

		// update the root pointer to point to the last page of the file
		int root = bf.numPages();
		int rootCategory = (root > 1 ? BTreePageId.INTERNAL : BTreePageId.LEAF);
		byte[] rootPtrBytes = convertToRootPtrPage(root, rootCategory, 0);
		bf.writePage(new BTreeRootPtrPage(BTreeRootPtrPage.getId(tableid), rootPtrBytes));

		// set all the parent and sibling pointers
		setParents(bf, new BTreePageId(tableid, root, rootCategory), BTreeRootPtrPage.getId(tableid));
		setRightSiblingPtrs(bf, lastPid, null);

		Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
		return bf;
	}

	/**
	 * Set all the right sibling pointers by following the left sibling pointers
	 * 
	 * @param bf - the BTreeFile
	 * @param pid - the id of the page to update with the right sibling pointer
	 * @param rightSiblingId - the id of the page's right sibling
	 * @throws IOException
	 * @throws DbException
	 */
	private static void setRightSiblingPtrs(BTreeFile bf, BTreePageId pid, BTreePageId rightSiblingId) 
			throws IOException, DbException {
		BTreeLeafPage page = (BTreeLeafPage) bf.readPage(pid);
		page.setRightSiblingId(rightSiblingId);
		BTreePageId leftSiblingId = page.getLeftSiblingId();
		bf.writePage(page);
		if(leftSiblingId != null) {
			setRightSiblingPtrs(bf, leftSiblingId, page.getId());
		}
	}

	/**
	 * Recursive function to set all the parent pointers
	 * 
	 * @param bf - the BTreeFile
	 * @param pid - id of the page to update with the parent pointer
	 * @param parent - the id of the page's parent
	 * @throws IOException
	 * @throws DbException
	 */
	private static void setParents(BTreeFile bf, BTreePageId pid, BTreePageId parent) 
			throws IOException, DbException {
		if(pid.pgcateg() == BTreePageId.INTERNAL) {
			BTreeInternalPage page = (BTreeInternalPage) bf.readPage(pid);
			page.setParentId(parent);

			Iterator<BTreeEntry> it = page.iterator();
			BTreeEntry e = null;
			while(it.hasNext()) {
				e = it.next();
				setParents(bf, e.getLeftChild(), pid);
			}
			if(e != null) {
				setParents(bf, e.getRightChild(), pid);
			}
			bf.writePage(page);
		}
		else { // pid.pgcateg() == BTreePageId.LEAF
			BTreeLeafPage page = (BTreeLeafPage) bf.readPage(pid);
			page.setParentId(parent);
			bf.writePage(page);
		}
	}

	/**
	 * Write out any remaining entries and update the parent pointers.
	 * 
	 * @param entries - the list of remaining entries
	 * @param bf - the BTreeFile
	 * @param nentries - number of entries per page
	 * @param npagebytes - number of bytes per page
	 * @param keyType - the type of the key field
	 * @param tableid - the table id of this BTreeFile
	 * @param keyField - the index of the key field
	 * @throws IOException
	 */
	private static void cleanUpEntries(ArrayList<ArrayList<BTreeEntry>> entries,
			BTreeFile bf, int nentries, int npagebytes, Type keyType, int tableid, 
			int keyField) throws IOException {
		// As with the leaf pages, there are two options:
		// 1. We have less than or equal to a full page of entries. Because of the way the code
		//    was written, we know this must be the root page
		// 2. We have somewhere between one and two pages of entries remaining.
		// For case (1), we write out the page 
		// For case (2), we divide the remaining entries equally between the last two pages,
		// write them out, and update the parent's child pointers.
		for(int i = 0; i < entries.size(); i++) {
			int childPageCategory = (i == 0 ? BTreePageId.LEAF : BTreePageId.INTERNAL);
			int size = entries.get(i).size();
			if(size <= nentries) {
				// write out a page of entries
				byte[] internalPageBytes = convertToInternalPage(entries.get(i), npagebytes, keyType, childPageCategory);
				BTreePageId internalPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.INTERNAL);
				bf.writePage(new BTreeInternalPage(internalPid, internalPageBytes, keyField));
			}
			else {
				// split the remaining entries in half
				ArrayList<BTreeEntry> secondToLastPg = new ArrayList<BTreeEntry>();
				ArrayList<BTreeEntry> lastPg = new ArrayList<BTreeEntry>();
				secondToLastPg.addAll(entries.get(i).subList(0, size/2));
				lastPg.addAll(entries.get(i).subList(size/2 + 1, size));

				// write out the last two pages of entries
				byte[] secondToLastPageBytes = convertToInternalPage(secondToLastPg, npagebytes, keyType, childPageCategory);
				BTreePageId secondToLastPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.INTERNAL);
				bf.writePage(new BTreeInternalPage(secondToLastPid, secondToLastPageBytes, keyField));

				byte[] lastPageBytes = convertToInternalPage(lastPg, npagebytes, keyType, childPageCategory);
				BTreePageId lastPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.INTERNAL);
				bf.writePage(new BTreeInternalPage(lastPid, lastPageBytes, keyField));

				// update the parent by "pushing up" the next key
				BTreeEntry pushUpEntry = new BTreeEntry(entries.get(i).get(size/2).getKey(), secondToLastPid, lastPid);
				updateEntries(entries, bf, pushUpEntry, i+1, nentries, npagebytes, 
						keyType, tableid, keyField);
			}

		}
	}

	/**
	 * Recursive function to update the entries by adding a new Entry at a particular level
	 * 
	 * @param entries - the list of entries
	 * @param bf - the BTreefile
	 * @param e - the new entry 
	 * @param level - the level of the new entry (0 is closest to the leaf pages)
	 * @param nentries - number of entries per page
	 * @param npagebytes - number of bytes per page
	 * @param keyType - the type of the key field
	 * @param tableid - the table id of this BTreeFile
	 * @param keyField - the index of the key field
	 * @throws IOException
	 */
	private static void updateEntries(ArrayList<ArrayList<BTreeEntry>> entries, 
			BTreeFile bf, BTreeEntry e, int level, int nentries, int npagebytes, Type keyType, 
			int tableid, int keyField) throws IOException {
		while(entries.size() <= level) {
			entries.add(new ArrayList<BTreeEntry>());
		}

		int childPageCategory = (level == 0 ? BTreePageId.LEAF : BTreePageId.INTERNAL);
		int size = entries.get(level).size();

		if(size > 0) {
			BTreeEntry prev = entries.get(level).get(size-1);
			entries.get(level).set(size-1, new BTreeEntry(prev.getKey(), prev.getLeftChild(), e.getLeftChild()));
			if(size == nentries * 2 + 1) {
				// write out a page of entries
				ArrayList<BTreeEntry> pageEntries = new ArrayList<BTreeEntry>();
				pageEntries.addAll(entries.get(level).subList(0, nentries));
				byte[] internalPageBytes = convertToInternalPage(pageEntries, npagebytes, keyType, childPageCategory);
				BTreePageId internalPid = new BTreePageId(tableid, bf.numPages() + 1, BTreePageId.INTERNAL);
				bf.writePage(new BTreeInternalPage(internalPid, internalPageBytes, keyField));

				// update the parent by "pushing up" the next key
				BTreeEntry pushUpEntry = new BTreeEntry(entries.get(level).get(nentries).getKey(), internalPid, null);
				updateEntries(entries, bf, pushUpEntry, level + 1, nentries, npagebytes, 
						keyType, tableid, keyField);
				ArrayList<BTreeEntry> remainingEntries = new ArrayList<BTreeEntry>();
				remainingEntries.addAll(entries.get(level).subList(nentries+1, size));
				entries.get(level).clear();
				entries.get(level).addAll(remainingEntries);
			}
		}
		entries.get(level).add(e);
	}

	/**
	 * Convert a set of tuples to a byte array in the format of a BTreeLeafPage
	 * 
	 * @param tuples - the set of tuples
	 * @param npagebytes - number of bytes per page
	 * @param numFields - number of fields in each tuple
	 * @param typeAr - array containing the types of the tuples
	 * @param keyField - the field of the tuples the B+ tree will be keyed on
	 * @return a byte array which can be passed to the BTreeLeafPage constructor
	 * @throws IOException
	 */
	public static byte[] convertToLeafPage(ArrayList<Tuple> tuples, int npagebytes,
			int numFields, Type[] typeAr, int keyField)
					throws IOException {
		int nrecbytes = 0;
		for (int i = 0; i < numFields ; i++) {
			nrecbytes += typeAr[i].getLen();
		}
		// pointerbytes: left sibling pointer, right sibling pointer, parent pointer
		int pointerbytes = 3 * BTreeLeafPage.INDEX_SIZE; 
		int nrecords = (npagebytes * 8 - pointerbytes * 8) /  (nrecbytes * 8 + 1);  //floor comes for free

		//  per record, we need one bit; there are nrecords per page, so we need
		// nrecords bits, i.e., ((nrecords/32)+1) integers.
		int nheaderbytes = (nrecords / 8);
		if (nheaderbytes * 8 < nrecords)
			nheaderbytes++;  //ceiling
		int nheaderbits = nheaderbytes * 8;

		ByteArrayOutputStream baos = new ByteArrayOutputStream(npagebytes);
		DataOutputStream dos = new DataOutputStream(baos);

		// write out the pointers and the header of the page,
		// then sort the tuples on the keyField and write out the tuples.
		//
		// in the header, write a 1 for bits that correspond to records we've
		// written and 0 for empty slots.

		int recordcount = tuples.size();
		if (recordcount > nrecords)
			recordcount = nrecords;

		dos.writeInt(0); // parent pointer
		dos.writeInt(0); // left sibling pointer
		dos.writeInt(0); // right sibling pointer

		int i = 0;
		byte headerbyte = 0;

		for (i=0; i<nheaderbits; i++) {
			if (i < recordcount)
				headerbyte |= (1 << (i % 8));

			if (((i+1) % 8) == 0) {
				dos.writeByte(headerbyte);
				headerbyte = 0;
			}
		}

		if (i % 8 > 0)
			dos.writeByte(headerbyte);

		Collections.sort(tuples, new TupleComparator(keyField));
		for(int t = 0; t < recordcount; t++) {
			TupleDesc td = tuples.get(t).getTupleDesc();
			for(int j = 0; j < td.numFields(); j++) {
				tuples.get(t).getField(j).serialize(dos);
			}
		}

		// pad the rest of the page with zeroes
		for (i=0; i<(npagebytes - (recordcount * nrecbytes + nheaderbytes + pointerbytes)); i++)
			dos.writeByte(0);

		return baos.toByteArray();
	}

	/**
	 *  Comparator to sort BTreeEntry objects by key
	 */
	public static class EntryComparator implements Comparator<BTreeEntry> {
		/**
		 * Compare two entries based on their key field
		 * 
		 * @return -1 if e1 < e2, 1 if e1 > e2, 0 if e1 == e2
		 */
		public int compare(BTreeEntry e1, BTreeEntry e2) {
			int cmp = 0;
			if(e1.getKey().compare(Op.LESS_THAN, e2.getKey())) {
				cmp = -1;
			}
			else if(e1.getKey().compare(Op.GREATER_THAN, e2.getKey())) {
				cmp = 1;
			}
			return cmp;
		}
	}

	/**
	 *  Comparator to sort BTreeEntry objects by key in descending order
	 */
	public static class ReverseEntryComparator implements Comparator<BTreeEntry> {
		/**
		 * Compare two entries based on their key field
		 * 
		 * @return -1 if e1 > e2, 1 if e1 < e2, 0 if e1 == e2
		 */
		public int compare(BTreeEntry e1, BTreeEntry e2) {
			int cmp = 0;
			if(e1.getKey().compare(Op.GREATER_THAN, e2.getKey())) {
				cmp = -1;
			}
			else if(e1.getKey().compare(Op.LESS_THAN, e2.getKey())) {
				cmp = 1;
			}
			return cmp;
		}
	}

	/**
	 * Convert a set of entries to a byte array in the format of a BTreeInternalPage
	 * 
	 * @param entries - the set of entries
	 * @param npagebytes - number of bytes per page
	 * @param keyType - the type of the key field
	 * @param childPageCategory - the category of the child pages (either internal or leaf)
	 * @return a byte array which can be passed to the BTreeInternalPage constructor
	 * @throws IOException
	 */
	public static byte[] convertToInternalPage(ArrayList<BTreeEntry> entries, int npagebytes,
			Type keyType, int childPageCategory)
					throws IOException {
		int nentrybytes = keyType.getLen() + BTreeInternalPage.INDEX_SIZE;
		// pointerbytes: one extra child pointer, parent pointer, child page category
		int pointerbytes = 2 * BTreeLeafPage.INDEX_SIZE + 1; 
		int nentries = (npagebytes * 8 - pointerbytes * 8 - 1) /  (nentrybytes * 8 + 1);  //floor comes for free

		//  per entry, we need one bit; there are nentries per page, so we need
		// nentries bits, plus 1 for the extra child pointer.
		int nheaderbytes = (nentries + 1) / 8;
		if (nheaderbytes * 8 < nentries + 1)
			nheaderbytes++;  //ceiling
		int nheaderbits = nheaderbytes * 8;

		ByteArrayOutputStream baos = new ByteArrayOutputStream(npagebytes);
		DataOutputStream dos = new DataOutputStream(baos);

		// write out the pointers and the header of the page,
		// then sort the entries and write them out.
		//
		// in the header, write a 1 for bits that correspond to entries we've
		// written and 0 for empty slots.
		int entrycount = entries.size();
		if (entrycount > nentries)
			entrycount = nentries;

		dos.writeInt(0); // parent pointer
		dos.writeByte((byte) childPageCategory);

		int i = 0;
		byte headerbyte = 0;

		for (i=0; i<nheaderbits; i++) {
			if (i < entrycount + 1)
				headerbyte |= (1 << (i % 8));

			if (((i+1) % 8) == 0) {
				dos.writeByte(headerbyte);
				headerbyte = 0;
			}
		}

		if (i % 8 > 0)
			dos.writeByte(headerbyte);

		Collections.sort(entries, new EntryComparator());
		for(int e = 0; e < entrycount; e++) {
			entries.get(e).getKey().serialize(dos);
		}

		for(int e = entrycount; e < nentries; e++) {
			for (int j=0; j<keyType.getLen(); j++) {
				dos.writeByte(0);
			}
		}

		dos.writeInt(entries.get(0).getLeftChild().getPageNumber());
		for(int e = 0; e < entrycount; e++) {
			dos.writeInt(entries.get(e).getRightChild().getPageNumber());
		}

		for(int e = entrycount; e < nentries; e++) {
			for (int j=0; j<BTreeInternalPage.INDEX_SIZE; j++) {
				dos.writeByte(0);
			}
		}

		// pad the rest of the page with zeroes
		for (i=0; i<(npagebytes - (nentries * nentrybytes + nheaderbytes + pointerbytes)); i++)
			dos.writeByte(0);

		return baos.toByteArray();

	}

	/**
	 * Create a byte array in the format of a BTreeRootPtrPage
	 * 
	 * @param root - the page number of the root page
	 * @param rootCategory - the category of the root page (leaf or internal)
	 * @param header - the page number of the first header page
	 * @return a byte array which can be passed to the BTreeRootPtrPage constructor
	 * @throws IOException
	 */
	public static byte[] convertToRootPtrPage(int root, int rootCategory, int header)
			throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream(BTreeRootPtrPage.getPageSize());
		DataOutputStream dos = new DataOutputStream(baos);

		dos.writeInt(root); // root pointer
		dos.writeByte((byte) rootCategory); // root page category

		dos.writeInt(header); // header pointer

		return baos.toByteArray();
	}

}
