package simpledb;

/** A LogicalScanNode represents table in the FROM list in a
 * LogicalQueryPlan */
public class LogicalScanNode {

    /** The name (alias) of the table as it is used in the query */
    public String alias;

    /** The table identifier (can be passed to {@link Catalog#getDbFile})
     *   to retrieve a DbFile */
    public int t;

    public LogicalScanNode(int table, String tableAlias) {
        this.alias = tableAlias;
        this.t = table;
    }
}

