package simpledb;

import java.io.File;
import java.io.IOException;

public class SimpleDb {
    public static void main(String args[])
            throws DbException, TransactionAbortedException, IOException {
        // convert a file
        if (args[0].equals("convert")) {
            try {
                if (args.length < 3 || args.length > 5) {
                    System.err.println("Unexpected number of arguments to convert ");
                    return;
                }
                File sourceTxtFile = new File(args[1]);
                File targetDatFile = new File(args[1].replaceAll(".txt", ".dat"));
                int numOfAttributes = Integer.parseInt(args[2]);
                Type[] ts = new Type[numOfAttributes];
                char fieldSeparator = ',';

                if (args.length == 3)
                    for (int i = 0; i < numOfAttributes; i++)
                        ts[i] = Type.INT_TYPE;
                else {
                    String typeString = args[3];
                    String[] typeStringAr = typeString.split(",");
                    if (typeStringAr.length != numOfAttributes) {
                        System.err.println("The number of types does not agree with the number of columns");
                        return;
                    }
                    int index = 0;
                    for (String s : typeStringAr) {
                        if (s.toLowerCase().equals("int"))
                            ts[index++] = Type.INT_TYPE;
                        else if (s.toLowerCase().equals("string"))
                            ts[index++] = Type.STRING_TYPE;
                        else {
                            System.err.println("Unknown type " + s);
                            return;
                        }
                    }
                    if (args.length == 5)
                        fieldSeparator = args[4].charAt(0);
                }

                HeapFileEncoder.convert(sourceTxtFile, targetDatFile,
                        BufferPool.PAGE_SIZE, numOfAttributes, ts, fieldSeparator);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (args[0].equals("print")) {
            File tableFile = new File(args[1]);
            int columns = Integer.parseInt(args[2]);
            DbFile table = Utility.openHeapFile(columns, tableFile);
            TransactionId tid = new TransactionId();
            DbFileIterator it = table.iterator(tid);

            if (null == it) {
                System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
            } else {
                it.open();
                while (it.hasNext()) {
                    Tuple t = it.next();
                    System.out.println(t);
                }
                it.close();
            }
        } else if (args[0].equals("parser")) {
            // Strip the first argument and call the parser
            String[] newargs = new String[args.length - 1];
            for (int i = 1; i < args.length; ++i) {
                newargs[i - 1] = args[i];
            }

            try {
                //dynamically load Parser -- if it doesn't exist, print error message
                Class<?> c = Class.forName("simpledb.Parser");
                Class<?> s = String[].class;

                java.lang.reflect.Method m = c.getMethod("main", s);
                m.invoke(null, (java.lang.Object) newargs);
            } catch (ClassNotFoundException cne) {
                System.out.println("Class Parser not found -- perhaps you are trying to run the parser as a part of lab1?");
            } catch (Exception e) {
                System.out.println("Error in parser.");
                e.printStackTrace();
            }

        } else {
            System.err.println("Unknown command: " + args[0]);
            System.exit(1);
        }
    }

}
