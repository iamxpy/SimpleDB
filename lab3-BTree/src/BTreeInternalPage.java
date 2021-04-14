package simpledb;

import java.util.*;
import java.io.*;

import simpledb.Predicate.Op;

/**
 * Each instance of BTreeInternalPage stores data for one page of a BTreeFile and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see BTreeFile
 * @see BufferPool
 *
 */
public class BTreeInternalPage extends BTreePage {
	private final byte header[];
	private final Field keys[];
	private final int children[];
	private final int numSlots;
	
	private int childCategory; // either leaf or internal

	public void checkRep(Field lowerBound, Field upperBound, boolean checkOccupancy, int depth) {
		Field prev = lowerBound;
		assert(this.getId().pgcateg() == BTreePageId.INTERNAL);

		Iterator<BTreeEntry> it  = this.iterator();
		while (it.hasNext()) {
			Field f = it.next().getKey();
			assert(null == prev || prev.compare(Op.LESS_THAN_OR_EQ,f));
			prev = f;
		}

		if (null != upperBound && null != prev){
			assert(prev.compare(Op.LESS_THAN_OR_EQ, upperBound));
		}

		if (checkOccupancy && depth > 0) {
			assert (getNumEntries() >= getMaxEntries() / 2);
		}
	}
	
	/**
	 * Create a BTreeInternalPage from a set of bytes of data read from disk.
	 * The format of a BTreeInternalPage is a set of header bytes indicating
	 * the slots of the page that are in use, some number of entry slots, and extra
	 * bytes for the parent pointer, one extra child pointer (a node with m entries 
	 * has m+1 pointers to children), and the category of all child pages (either 
	 * leaf or internal).
	 *  Specifically, the number of entries is equal to: <p>
	 *          floor((BufferPool.getPageSize()*8 - extra bytes*8) / (entry size * 8 + 1))
	 * <p> where entry size is the size of entries in this index node
	 * (key + child pointer), which can be determined via the key field and 
	 * {@link Catalog#getTupleDesc}.
	 * The number of 8-bit header words is equal to:
	 * <p>
	 *      ceiling((no. entry slots + 1) / 8)
	 * <p>
	 * @see Database#getCatalog
	 * @see Catalog#getTupleDesc
	 * @see BufferPool#getPageSize()
	 * 
	 * @param id - the id of this page
	 * @param data - the raw data of this page
	 * @param key - the field which the index is keyed on
	 */
	public BTreeInternalPage(BTreePageId id, byte[] data, int key) throws IOException {
		super(id, key);
		this.numSlots = getMaxEntries() + 1;
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// Read the parent pointer
		try {
			Field f = Type.INT_TYPE.parse(dis);
			this.parent = ((IntField) f).getValue();
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}

		// read the child page category
		childCategory = (int) dis.readByte();

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		keys = new Field[numSlots];
		try{
			// allocate and read the keys of this page
			// start from 1 because the first key slot is not used
			// since a node with m keys has m+1 pointers
			keys[0] = null;
			for (int i=1; i<keys.length; i++)
				keys[i] = readNextKey(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}

		children = new int[numSlots];
		try{
			// allocate and read the child pointers of this page
			for (int i=0; i<children.length; i++)
				children[i] = readNextChild(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();

		setBeforeImage();
	}

	/** 
	 * Retrieve the maximum number of entries this page can hold. (The number of keys)
 	 */
	public int getMaxEntries() {        
		int keySize = td.getFieldType(keyField).getLen();
		int bitsPerEntryIncludingHeader = keySize * 8 + INDEX_SIZE * 8 + 1;
		// extraBits are: one parent pointer, 1 byte for child page category, 
		// one extra child pointer (node with m entries has m+1 pointers to children), 1 bit for extra header
		int extraBits = 2 * INDEX_SIZE * 8 + 8 + 1; 
		int entriesPerPage = (BufferPool.getPageSize()*8 - extraBits) / bitsPerEntryIncludingHeader; //round down
		return entriesPerPage;
	}

	/**
	 * Computes the number of bytes in the header of a B+ internal page with each entry occupying entrySize bytes
	 * @return the number of bytes in the header
	 */
	private int getHeaderSize() {        
		int slotsPerPage = getMaxEntries() + 1;
		int hb = (slotsPerPage / 8);
		if (hb * 8 < slotsPerPage) hb++;

		return hb;
	}

	/** Return a view of this page before it was modified
        -- used by recovery */
	public BTreeInternalPage getBeforeImage(){
		try {
			byte[] oldDataRef = null;
			synchronized(oldDataLock)
			{
				oldDataRef = oldData;
			}
			return new BTreeInternalPage(pid,oldDataRef,keyField);
		} catch (IOException e) {
			e.printStackTrace();
			//should never happen -- we parsed it OK before!
			System.exit(1);
		}
		return null;
	}

	public void setBeforeImage() {
		synchronized(oldDataLock)
		{
			oldData = getPageData().clone();
		}
	}

	/**
	 * Read keys from the source file.
	 */
	private Field readNextKey(DataInputStream dis, int slotId) throws NoSuchElementException {
		// if associated bit is not set, read forward to the next key, and
		// return null.
		if (!isSlotUsed(slotId)) {
			for (int i=0; i<td.getFieldType(keyField).getLen(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty key");
				}
			}
			return null;
		}

		// read the key field
		Field f = null;
		try {
			f = td.getFieldType(keyField).parse(dis);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}

		return f;
	}

	/**
	 * Read child pointers from the source file.
	 */
	private int readNextChild(DataInputStream dis, int slotId) throws NoSuchElementException {
		// if associated bit is not set, read forward to the next child pointer, and
		// return -1.
		if (!isSlotUsed(slotId)) {
			for (int i=0; i<INDEX_SIZE; i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty child pointer");
				}
			}
			return -1;
		}

		// read child pointer
		int child = -1;
		try {
			Field f = Type.INT_TYPE.parse(dis);
			child = ((IntField) f).getValue();
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}

		return child;
	}

	/**
	 * Generates a byte array representing the contents of this page.
	 * Used to serialize this page to disk.
	 * <p>
	 * The invariant here is that it should be possible to pass the byte
	 * array generated by getPageData to the BTreeInternalPage constructor and
	 * have it produce an identical BTreeInternalPage object.
	 *
	 * @see #BTreeInternalPage
	 * @return A byte array correspond to the bytes of this page.
	 */
	public byte[] getPageData() {
		int len = BufferPool.getPageSize();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// write out the parent pointer
		try {
			dos.writeInt(parent);

		} catch (IOException e) {
			e.printStackTrace();
		}

		// write out the child page category
		try {
			dos.writeByte((byte) childCategory);

		} catch (IOException e) {
			e.printStackTrace();
		}

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the keys
		// start from 1 because the first key slot is not used
		// since a node with m keys has m+1 pointers
		for (int i=1; i<keys.length; i++) {

			// empty slot
			if (!isSlotUsed(i)) {
				for (int j=0; j<td.getFieldType(keyField).getLen(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			try {
				keys[i].serialize(dos);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		// create the child pointers
		for (int i=0; i<children.length; i++) {

			// empty slot
			if (!isSlotUsed(i)) {
				for (int j=0; j<INDEX_SIZE; j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			try {
				dos.writeInt(children[i]);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// padding
		int zerolen = BufferPool.getPageSize() - (INDEX_SIZE + 1 + header.length + 
				td.getFieldType(keyField).getLen() * (keys.length - 1) + INDEX_SIZE * children.length); 
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Delete the specified entry (key + 1 child pointer) from the page. The recordId
	 * is used to find the specified entry, so it must not be null. After deletion, the 
	 * entry's recordId should be set to null to reflect that it is no longer stored on 
	 * any page.
	 * @throws DbException if this entry is not on this page, or entry slot is
	 *         already empty.
	 * @param e The entry to delete
	 * @param deleteRightChild - if true, delete the right child. Otherwise
	 *        delete the left child
	 */
	private void deleteEntry(BTreeEntry e, boolean deleteRightChild) throws DbException {
		RecordId rid = e.getRecordId();
		if(rid == null)
			throw new DbException("tried to delete entry with null rid");
		if((rid.getPageId().getPageNumber() != pid.getPageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
			throw new DbException("tried to delete entry on invalid page or table");
		if (!isSlotUsed(rid.getTupleNumber()))
			throw new DbException("tried to delete null entry.");
		if(deleteRightChild) {
			markSlotUsed(rid.getTupleNumber(), false);
		}
		else {
			for(int i = rid.getTupleNumber() - 1; i >= 0; i--) {
				if(isSlotUsed(i)) {
					children[i] = children[rid.getTupleNumber()];
					markSlotUsed(rid.getTupleNumber(), false);
					break;
				}	
			}
		}
		e.setRecordId(null);
	}

	/**
	 * Delete the specified entry (key + right child pointer) from the page. The recordId
	 * is used to find the specified entry, so it must not be null. After deletion, the 
	 * entry's recordId should be set to null to reflect that it is no longer stored on 
	 * any page.
	 * @throws DbException if this entry is not on this page, or entry slot is
	 *         already empty.
	 * @param e The entry to delete
	 */
	public void deleteKeyAndRightChild(BTreeEntry e) throws DbException {
		deleteEntry(e, true);
	}
	
	/**
	 * Delete the specified entry (key + left child pointer) from the page. The recordId
	 * is used to find the specified entry, so it must not be null. After deletion, the 
	 * entry's recordId should be set to null to reflect that it is no longer stored on 
	 * any page.
	 * @throws DbException if this entry is not on this page, or entry slot is
	 *         already empty.
	 * @param e The entry to delete
	 */
	public void deleteKeyAndLeftChild(BTreeEntry e) throws DbException {
		deleteEntry(e, false);
	}
	
	/**
	 * Update the key and/or child pointers of an entry at the location specified by its 
	 * record id.
	 * @param e - the entry with updated key and/or child pointers
	 * @throws DbException if this entry is not on this page, entry slot is
	 *         already empty, or updating this key would put the entry out of 
	 *         order on the page
	 */
	public void updateEntry(BTreeEntry e) throws DbException {
		RecordId rid = e.getRecordId();
		if(rid == null)
			throw new DbException("tried to update entry with null rid");
		if((rid.getPageId().getPageNumber() != pid.getPageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
			throw new DbException("tried to update entry on invalid page or table");
		if (!isSlotUsed(rid.getTupleNumber()))
			throw new DbException("tried to update null entry.");
		
		for(int i = rid.getTupleNumber() + 1; i < numSlots; i++) {
			if(isSlotUsed(i)) {
				if(keys[i].compare(Op.LESS_THAN, e.getKey())) {
					throw new DbException("attempt to update entry with invalid key " + e.getKey() +
							" HINT: updated key must be less than or equal to keys on the right");
				}
				break;
			}	
		}
		for(int i = rid.getTupleNumber() - 1; i >= 0; i--) {
			if(isSlotUsed(i)) {
				if(i > 0 && keys[i].compare(Op.GREATER_THAN, e.getKey())) {
					throw new DbException("attempt to update entry with invalid key " + e.getKey() +
							" HINT: updated key must be greater than or equal to keys on the left");
				}
				children[i] = e.getLeftChild().getPageNumber();
				break;
			}	
		}
		children[rid.getTupleNumber()] = e.getRightChild().getPageNumber();
		keys[rid.getTupleNumber()] = e.getKey();
	}

	/**
	 * Adds the specified entry to the page; the entry's recordId should be updated to 
	 * reflect that it is now stored on this page.
	 * @throws DbException if the page is full (no empty slots) or key field type,
	 *         table id, or child page category is a mismatch, or the entry is invalid
	 * @param e The entry to add.
	 */
	public void insertEntry(BTreeEntry e) throws DbException {
		if (!e.getKey().getType().equals(td.getFieldType(keyField)))
			throw new DbException("key field type mismatch, in insertEntry");

		if(e.getLeftChild().getTableId() != pid.getTableId() || e.getRightChild().getTableId() != pid.getTableId())
			throw new DbException("table id mismatch in insertEntry");

		if(childCategory == 0) {
			if(e.getLeftChild().pgcateg() != e.getRightChild().pgcateg())
				throw new DbException("child page category mismatch in insertEntry");

			childCategory = e.getLeftChild().pgcateg();
		}
		else if(e.getLeftChild().pgcateg() != childCategory || e.getRightChild().pgcateg() != childCategory)
			throw new DbException("child page category mismatch in insertEntry");

		// if this is the first entry, add it and return
		if(getNumEmptySlots() == getMaxEntries()) {
			children[0] = e.getLeftChild().getPageNumber();
			children[1] = e.getRightChild().getPageNumber();
			keys[1] = e.getKey();
			markSlotUsed(0, true);
			markSlotUsed(1, true);
			e.setRecordId(new RecordId(pid, 1));
			return;
		}

		// find the first empty slot, starting from 1
		int emptySlot = -1;
		for (int i=1; i<numSlots; i++) {
			if (!isSlotUsed(i)) {
				emptySlot = i;
				break;
			}
		}

		if (emptySlot == -1)
			throw new DbException("called insertEntry on page with no empty slots.");        

		// find the child pointer matching the left or right child in this entry
		int lessOrEqKey = -1;
		for (int i=0; i<numSlots; i++) {
			if(isSlotUsed(i)) {
				if(children[i] == e.getLeftChild().getPageNumber() || children[i] == e.getRightChild().getPageNumber()) {
					if(i > 0 && keys[i].compare(Op.GREATER_THAN, e.getKey())) {
						throw new DbException("attempt to insert invalid entry with left child " + 
								e.getLeftChild().getPageNumber() + ", right child " +
								e.getRightChild().getPageNumber() + " and key " + e.getKey() +
								" HINT: one of these children must match an existing child on the page" +
								" and this key must be correctly ordered in between that child's" +
								" left and right keys");
					}
					lessOrEqKey = i;
					if(children[i] == e.getRightChild().getPageNumber()) {
						children[i] = e.getLeftChild().getPageNumber();
					}
				}
				else if(lessOrEqKey != -1) {
					// validate that the next key is greater than or equal to the one we are inserting
					if(keys[i].compare(Op.LESS_THAN, e.getKey())) {
						throw new DbException("attempt to insert invalid entry with left child " + 
								e.getLeftChild().getPageNumber() + ", right child " +
								e.getRightChild().getPageNumber() + " and key " + e.getKey() +
								" HINT: one of these children must match an existing child on the page" +
								" and this key must be correctly ordered in between that child's" +
								" left and right keys");
					}
					break;
				}
			}
		}

		if(lessOrEqKey == -1) {
			throw new DbException("attempt to insert invalid entry with left child " + 
					e.getLeftChild().getPageNumber() + ", right child " +
					e.getRightChild().getPageNumber() + " and key " + e.getKey() +
					" HINT: one of these children must match an existing child on the page" +
					" and this key must be correctly ordered in between that child's" +
					" left and right keys");
		}

		// shift entries back or forward to fill empty slot and make room for new entry
		// while keeping entries in sorted order
		int goodSlot = -1;
		if(emptySlot < lessOrEqKey) {
			for(int i = emptySlot; i < lessOrEqKey; i++) {
				moveEntry(i+1, i);
			}
			goodSlot = lessOrEqKey;
		}
		else {
			for(int i = emptySlot; i > lessOrEqKey + 1; i--) {
				moveEntry(i-1, i);
			}
			goodSlot = lessOrEqKey + 1;
		}

		// insert new entry into the correct spot in sorted order
		markSlotUsed(goodSlot, true);
		Debug.log(1, "BTreeLeafPage.insertEntry: new entry, tableId = %d pageId = %d slotId = %d", pid.getTableId(), pid.getPageNumber(), goodSlot);
		keys[goodSlot] = e.getKey();
		children[goodSlot] = e.getRightChild().getPageNumber();
		e.setRecordId(new RecordId(pid, goodSlot));
	}

	/**
	 * Move an entry from one slot to another slot, and update the corresponding
	 * headers
	 */
	private void moveEntry(int from, int to) {
		if(!isSlotUsed(to) && isSlotUsed(from)) {
			markSlotUsed(to, true);
			keys[to] = keys[from];
			children[to] = children[from];
			markSlotUsed(from, false);
		}
	}

	/**
	 * Returns the number of entries (keys) currently stored on this page
	 */
	public int getNumEntries() {
		return numSlots - getNumEmptySlots() - 1;
	}
	
	/**
	 * Returns the number of empty slots on this page.
	 */
	public int getNumEmptySlots() {
		int cnt = 0;
		// start from 1 because the first key slot is not used
		// since a node with m keys has m+1 pointers
		for(int i=1; i<numSlots; i++)
			if(!isSlotUsed(i))
				cnt++;
		return cnt;
	}

	/**
	 * Returns true if associated slot on this page is filled.
	 */
	public boolean isSlotUsed(int i) {
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;
		return (header[headerbyte] & (1 << headerbit)) != 0;
	}

	/**
	 * Abstraction to fill or clear a slot on this page.
	 */
	private void markSlotUsed(int i, boolean value) {
		int headerbit = i % 8;
		int headerbyte = (i - headerbit) / 8;

		Debug.log(1, "BTreeInternalPage.setSlot: setting slot %d to %b", i, value);
		if(value)
			header[headerbyte] |= 1 << headerbit;
		else
			header[headerbyte] &= (0xFF ^ (1 << headerbit));
	}

	/**
	 * @return an iterator over all entries on this page (calling remove on this iterator throws an UnsupportedOperationException)
	 * (note that this iterator shouldn't return entries in empty slots!)
	 */
	public Iterator<BTreeEntry> iterator() {
		return new BTreeInternalPageIterator(this);
	}
	
	/**
	 * @return a reverse iterator over all entries on this page (calling remove on this iterator throws an UnsupportedOperationException)
	 * (note that this iterator shouldn't return entries in empty slots!)
	 */
	public Iterator<BTreeEntry> reverseIterator() {
		return new BTreeInternalPageReverseIterator(this);
	}

	/**
	 * protected method used by the iterator to get the ith key out of this page
	 * @param i - the index of the key
	 * @return the ith key
	 * @throws NoSuchElementException
	 */
	protected Field getKey(int i) throws NoSuchElementException {

		// key at slot 0 is not used
		if (i <= 0 || i >= keys.length)
			throw new NoSuchElementException();

		try {
			if(!isSlotUsed(i)) {
				Debug.log(1, "BTreeInternalPage.getKey: slot %d in %d:%d is not used", i, pid.getTableId(), pid.getPageNumber());
				return null;
			}

			Debug.log(1, "BTreeInternalPage.getKey: returning key %d", i);
			return keys[i];

		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * protected method used by the iterator to get the ith child page id out of this page
	 * @param i - the index of the child page id
	 * @return the ith child page id
	 * @throws NoSuchElementException
	 */
	protected BTreePageId getChildId(int i) throws NoSuchElementException {

		if (i < 0 || i >= children.length)
			throw new NoSuchElementException();

		try {
			if(!isSlotUsed(i)) {
				Debug.log(1, "BTreeInternalPage.getChildId: slot %d in %d:%d is not used", i, pid.getTableId(), pid.getPageNumber());
				return null;
			}

			Debug.log(1, "BTreeInternalPage.getChildId: returning child id %d", i);
			return new BTreePageId(pid.getTableId(), children[i], childCategory);

		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}
}

/**
 * Helper class that implements the Java Iterator for entries on a BTreeInternalPage.
 */
class BTreeInternalPageIterator implements Iterator<BTreeEntry> {
	int curEntry = 1;
	BTreePageId prevChildId = null;
	BTreeEntry nextToReturn = null;
	BTreeInternalPage p;

	public BTreeInternalPageIterator(BTreeInternalPage p) {
		this.p = p;
	}

	public boolean hasNext() {
		if (nextToReturn != null)
			return true;

		try {
			if(prevChildId == null) {
				prevChildId = p.getChildId(0);
				if(prevChildId == null) {
					return false;
				}
			}
			while (true) {
				int entry = curEntry++;
				Field key = p.getKey(entry);
				BTreePageId childId = p.getChildId(entry);
				if(key != null && childId != null) {
					nextToReturn = new BTreeEntry(key, prevChildId, childId);
					nextToReturn.setRecordId(new RecordId(p.pid, entry));
					prevChildId = childId;
					return true;
				}
			}
		} catch(NoSuchElementException e) {
			return false;
		}
	}

	public BTreeEntry next() {
		BTreeEntry next = nextToReturn;

		if (next == null) {
			if (hasNext()) {
				next = nextToReturn;
				nextToReturn = null;
				return next;
			} else
				throw new NoSuchElementException();
		} else {
			nextToReturn = null;
			return next;
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}

/**
 * Helper class that implements the Java Iterator for entries on a BTreeInternalPage in reverse.
 */
class BTreeInternalPageReverseIterator implements Iterator<BTreeEntry> {
	int curEntry;
	BTreePageId nextChildId = null;
	BTreeEntry nextToReturn = null;
	BTreeInternalPage p;

	public BTreeInternalPageReverseIterator(BTreeInternalPage p) {
		this.p = p;
		this.curEntry = p.getMaxEntries();
		while(!p.isSlotUsed(curEntry) && curEntry > 0) {
			--curEntry;
		}
	}

	public boolean hasNext() {
		if (nextToReturn != null)
			return true;

		try {
			if(nextChildId == null) {
				nextChildId = p.getChildId(curEntry);
				if(nextChildId == null) {
					return false;
				}
			}
			while (true) {
				int entry = curEntry--;
				Field key = p.getKey(entry);
				BTreePageId childId = p.getChildId(entry - 1);
				if(key != null && childId != null) {
					nextToReturn = new BTreeEntry(key, childId, nextChildId);
					nextToReturn.setRecordId(new RecordId(p.pid, entry));
					nextChildId = childId;
					return true;
				}
			}
		} catch(NoSuchElementException e) {
			return false;
		}
	}

	public BTreeEntry next() {
		BTreeEntry next = nextToReturn;

		if (next == null) {
			if (hasNext()) {
				next = nextToReturn;
				nextToReturn = null;
				return next;
			} else
				throw new NoSuchElementException();
		} else {
			nextToReturn = null;
			return next;
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}