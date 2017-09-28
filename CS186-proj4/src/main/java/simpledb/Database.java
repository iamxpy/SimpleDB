package simpledb;

import java.io.*;

/** Database is a class that initializes several static
    variables used by the database system (the catalog, the buffer pool,
    and the log files, in particular.)
    <p>
    Provides a set of methods that can be used to access these variables
    from anywhere.
*/

public class Database {
	private static Database _instance = new Database();
    private final Catalog _catalog;
    private BufferPool _bufferpool; 

    private final static String LOGFILENAME = "log";
    private LogFile _logfile;

    private Database() {
    	_catalog = new Catalog();
    	_bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
    	try {
            _logfile = new LogFile(new File(LOGFILENAME));
        } catch(IOException e) {
            _logfile = null;
            e.printStackTrace();
            System.exit(1);
        }
        // startControllerThread();
    }

    /** Return the log file of the static Database instance*/
    public static LogFile getLogFile() {
        return _instance._logfile;
    }

    /** Return the buffer pool of the static Database instance*/
    public static BufferPool getBufferPool() {
        return _instance._bufferpool;
    }

    /** Return the catalog of the static Database instance*/
    public static Catalog getCatalog() {
        return _instance._catalog;
    }

    /** Method used for testing -- create a new instance of the
        buffer pool and return it
    */
    public static BufferPool resetBufferPool(int pages) {
        _instance._bufferpool = new BufferPool(pages);
        return _instance._bufferpool;
    }

    //reset the database, used for unit tests only.
    public static void reset() {
    	_instance = new Database();
    }

}
