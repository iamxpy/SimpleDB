package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.UUID;

/** Helper methods used for testing and implementing random features. */
public class Utility {
    /**
     * @return a Type array of length len populated with Type.INT_TYPE
     */
    public static Type[] getTypes(int len) {
        Type[] types = new Type[len];
        for (int i = 0; i < len; ++i)
            types[i] = Type.INT_TYPE;
        return types;
    }

    /**
     * @return a String array of length len populated with the (possibly null) strings in val,
     * and an appended increasing integer at the end (val1, val2, etc.).
     */
    public static String[] getStrings(int len, String val) {
        String[] strings = new String[len];
        for (int i = 0; i < len; ++i)
            strings[i] = val + i;
        return strings;
    }

    /**
     * @return a TupleDesc with n fields of type Type.INT_TYPE, each named
     * name + n (name1, name2, etc.).
     */
    public static TupleDesc getTupleDesc(int n, String name) {
        return new TupleDesc(getTypes(n), getStrings(n, name));
    }

    /**
     * @return a TupleDesc with n fields of type Type.INT_TYPE
     */
    public static TupleDesc getTupleDesc(int n) {
        return new TupleDesc(getTypes(n));
    }

    /**
     * @return a Tuple with a single IntField with value n and with
     *   RecordId(HeapPageId(1,2), 3)
     */
    public static Tuple getHeapTuple(int n) {
        Tuple tup = new Tuple(getTupleDesc(1));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        tup.setField(0, new IntField(n));
        return tup;
    }

    /**
     * @return a Tuple with an IntField for every element of tupdata
     *   and RecordId(HeapPageId(1, 2), 3)
     */
    public static Tuple getHeapTuple(int[] tupdata) {
        Tuple tup = new Tuple(getTupleDesc(tupdata.length));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        for (int i = 0; i < tupdata.length; ++i)
            tup.setField(i, new IntField(tupdata[i]));
        return tup;
    }

    /**
     * @return a Tuple with a 'width' IntFields each with value n and
     *   with RecordId(HeapPageId(1, 2), 3)
     */
    public static Tuple getHeapTuple(int n, int width) {
        Tuple tup = new Tuple(getTupleDesc(width));
        tup.setRecordId(new RecordId(new HeapPageId(1, 2), 3));
        for (int i = 0; i < width; ++i)
            tup.setField(i, new IntField(n));
        return tup;
    }

    /**
     * @return a Tuple with a 'width' IntFields with the value tupledata[i]
     *         in each field.
     *         do not set it's RecordId, hence do not distinguish which
     *         sort of file it belongs to.
     */
    public static Tuple getTuple(int[] tupledata, int width) {
        if(tupledata.length != width) {
            System.out.println("get Hash Tuple has the wrong length~");
            System.exit(1);
        }
        Tuple tup = new Tuple(getTupleDesc(width));
        for (int i = 0; i < width; ++i)
            tup.setField(i, new IntField(tupledata[i]));
        return tup;
    }

    /**
     * A utility method to create a new HeapFile with a single empty page,
     * assuming the path does not already exist. If the path exists, the file
     * will be overwritten. The new table will be added to the Catalog with
     * the specified number of columns as IntFields.
     */
    public static HeapFile createEmptyHeapFile(String path, int cols)
        throws IOException {
        File f = new File(path);
        // touch the file
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(new byte[0]);
        fos.close();

        HeapFile hf = openHeapFile(cols, f);
        HeapPageId pid = new HeapPageId(hf.getId(), 0);

        HeapPage page = null;
        try {
            page = new HeapPage(pid, HeapPage.createEmptyPageData());
        } catch (IOException e) {
            // this should never happen for an empty page; bail;
            throw new RuntimeException("failed to create empty page in HeapFile");
        }

        hf.writePage(page);
        return hf;
    }

    /** Opens a HeapFile and adds it to the catalog.
     *
     * @param cols number of columns in the table.
     * @param f location of the file storing the table.
     * @return the opened table.
     */
    public static HeapFile openHeapFile(int cols, File f) {
        // create the HeapFile and add it to the catalog
    	TupleDesc td = getTupleDesc(cols);
        HeapFile hf = new HeapFile(f, td);
        Database.getCatalog().addTable(hf, UUID.randomUUID().toString());
        return hf;
    }
    
    public static HeapFile openHeapFile(int cols, String colPrefix, File f) {
        // create the HeapFile and add it to the catalog
    	TupleDesc td = getTupleDesc(cols, colPrefix);
        HeapFile hf = new HeapFile(f, td);
        Database.getCatalog().addTable(hf, UUID.randomUUID().toString());
        return hf;
    }

    public static String listToString(ArrayList<Integer> list) {
        String out = "";
        for (Integer i : list) {
            if (out.length() > 0) out += "\t";
            out += i;
        }
        return out;
    }
}

