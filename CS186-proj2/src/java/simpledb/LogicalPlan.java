package simpledb;
import java.util.Map;
import java.util.Vector;
import java.util.HashMap;
import java.util.Iterator;
import java.io.File;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * LogicalPlan represents a logical query plan that has been through
 * the parser and is ready to be processed by the optimizer.
 * <p>
 * A LogicalPlan consits of a collection of table scan nodes, join
 * nodes, filter nodes, a select list, and a group by field.
 * LogicalPlans can only represent queries with one aggregation field
 * and one group by field.
 * <p>
 * LogicalPlans can be converted to physical (optimized) plans using
 * the {@link #physicalPlan} method, which uses the
 * {@link JoinOptimizer} to order joins optimally and to select the
 * best implementations for joins.
 */
public class LogicalPlan {
    private Vector<LogicalJoinNode> joins;
    private Vector<LogicalScanNode> tables;
    private Vector<LogicalFilterNode> filters;
    private HashMap<String,DbIterator> subplanMap;
    private HashMap<String,Integer> tableMap;

    private Vector<LogicalSelectListNode> selectList;
    private String groupByField = null;
    private boolean hasAgg = false;
    private String aggOp;
    private String aggField;
    private boolean oByAsc, hasOrderBy = false;
    private String oByField;
    private String query;
//    private Query owner;

    /** Constructor -- generate an empty logical plan */
    public LogicalPlan() {
        joins = new Vector<LogicalJoinNode>();
        filters = new Vector<LogicalFilterNode>();
        tables = new Vector<LogicalScanNode>();
        subplanMap = new HashMap<String,DbIterator>();
        tableMap = new HashMap<String,Integer>();

        selectList = new Vector<LogicalSelectListNode>();
        this.query = "";
    }

    /** Set the text of the query representing this logical plan.  Does NOT parse the
        specified query -- this method is just used so that the object can print the
        SQL it represents.

        @param query the text of the query associated with this plan
    */
    public void setQuery(String query)  {
        this.query = query;
    }
      
    /** Get the query text associated with this plan via {@link #setQuery}.
     */
    public String getQuery() {
        return query;
    }

    /** Given a table alias, return id of the table object (this id can be supplied to {@link Catalog#getDbFile(int)}).
        Aliases are added as base tables are added via {@link #addScan}.

        @param alias the table alias to return a table id for
        @return the id of the table corresponding to alias, or null if the alias is unknown
     */
    public Integer getTableId(String alias) {
        return tableMap.get(alias);
    }
    
    public HashMap<String,Integer> getTableAliasToIdMapping()
    {
        return this.tableMap;
    }

    /** Add a new filter to the logical plan
     *   @param field The name of the over which the filter applies;
     *   this can be a fully qualified field (tablename.field or
     *   alias.field), or can be a unique field name without a
     *   tablename qualifier.  If it is an ambiguous name, it will
     *   throw a ParsingException
     *   @param p The predicate for the filter
     *   @param constantValue the constant to compare the predicate
     *   against; if field is an integer field, this should be a
     *   String representing an integer
     *   @throws ParsingException if field is not in one of the tables
     *   added via {@link #addScan} or if field is ambiguous (e.g., two
     *   tables contain a field named field.)
     */
    public void addFilter(String field, Predicate.Op p, String
        constantValue) throws ParsingException{ 

        field = disambiguateName(field); 
        String table = field.split("[.]")[0];
        
        LogicalFilterNode lf = new LogicalFilterNode(table, field.split("[.]")[1], p, constantValue);
        filters.addElement(lf);
    }

    /** Add a join between two fields of two different tables.  
     *  @param joinField1 The name of the first join field; this can
     *  be a fully qualified name (e.g., tableName.field or
     *  alias.field) or may be an unqualified unique field name.  If
     *  the name is ambiguous or unknown, a ParsingException will be
     *  thrown.
     *  @param joinField2 The name of the second join field
     *  @param pred The join predicate
     *  @throws ParsingException if either of the fields is ambiguous,
     *      or is not in one of the tables added via {@link #addScan}
    */

    public void addJoin( String joinField1, String joinField2, Predicate.Op pred) throws ParsingException {
        joinField1 = disambiguateName(joinField1);
        joinField2 = disambiguateName(joinField2);
        String table1Alias = joinField1.split("[.]")[0];
        String table2Alias = joinField2.split("[.]")[0];
        String pureField1 = joinField1.split("[.]")[1];
        String pureField2 = joinField2.split("[.]")[1];

        if (table1Alias.equals(table2Alias))
            throw new ParsingException("Cannot join on two fields from same table");
        LogicalJoinNode lj = new LogicalJoinNode(table1Alias,table2Alias,pureField1, pureField2, pred);
        System.out.println("Added join between " + joinField1 + " and " + joinField2);
        joins.addElement(lj);

    }

    /** Add a join between a field and a subquery.
     *  @param joinField1 The name of the first join field; this can
     *  be a fully qualified name (e.g., tableName.field or
     *  alias.field) or may be an unqualified unique field name.  If
     *  the name is ambiguous or unknown, a ParsingException will be
     *  thrown.
     *  @param joinField2 the subquery to join with -- the join field
     *    of the subquery is the first field in the result set of the query
     *  @param pred The join predicate.
     *  @throws ParsingException if either of the fields is ambiguous,
     *      or is not in one of the tables added via {@link #addScan}
     */
    public void addJoin( String joinField1, DbIterator joinField2, Predicate.Op pred) throws ParsingException {
        joinField1 = disambiguateName(joinField1);

        String table1 = joinField1.split("[.]")[0];
        String pureField = joinField1.split("[.]")[1];

        LogicalSubplanJoinNode lj = new LogicalSubplanJoinNode(table1,pureField, joinField2, pred);
        System.out.println("Added subplan join on " + joinField1);
        joins.addElement(lj);
    }

    /** Add a scan to the plan. One scan node needs to be added for each alias of a table
        accessed by the plan.
        @param table the id of the table accessed by the plan (can be resolved to a DbFile using {@link Catalog#getDbFile}
        @param name the alias of the table in the plan
    */

    public void addScan(int table, String name) {
        System.out.println("Added scan of table " + name);
        tables.addElement(new LogicalScanNode(table,name));
        tableMap.put(name,table);
    }

    /** Add a specified field/aggregate combination to the select list of the query.
        Fields are output by the query such that the rightmost field is the first added via addProjectField.
        @param fname the field to add to the output
        @param aggOp the aggregate operation over the field.
     * @throws ParsingException 
    */
    public void addProjectField(String fname, String aggOp) throws ParsingException {
        fname=disambiguateName(fname);
        if (fname.equals("*"))
            fname="null.*";
        System.out.println("Added select list field " + fname);
        if (aggOp != null) {
            System.out.println("\t with aggregator " + aggOp);
        }
        selectList.addElement(new LogicalSelectListNode(aggOp, fname));
    }
    
    /** Add an aggregate over the field with the specified grouping to
        the query.  SimpleDb only supports a single aggregate
        expression and GROUP BY field.
        @param op the aggregation operator
        @param afield the field to aggregate over
        @param gfield the field to group by
     * @throws ParsingException 
    */
    public void addAggregate(String op, String afield, String gfield) throws ParsingException {
        afield=disambiguateName(afield);
        if (gfield!=null)
            gfield=disambiguateName(gfield);
        aggOp = op;
        aggField = afield;
        groupByField = gfield;
        hasAgg = true;
    }

    /** Add an ORDER BY expression in the specified order on the specified field.  SimpleDb only supports
        a single ORDER BY field.
        @param field the field to order by
        @param asc true if should be ordered in ascending order, false for descending order
     * @throws ParsingException 
    */
    public void addOrderBy(String field, boolean asc) throws ParsingException {
        field=disambiguateName(field);
        oByField = field;
        oByAsc = asc;
        hasOrderBy = true;
    }

    /** Given a name of a field, try to figure out what table it belongs to by looking
     *   through all of the tables added via {@link #addScan}. 
     *  @return A fully qualified name of the form tableAlias.name.  If the name parameter is already qualified
     *   with a table name, simply returns name.
     *  @throws ParsingException if the field cannot be found in any of the tables, or if the
     *   field is ambiguous (appears in multiple tables)
     */
    String disambiguateName(String name) throws ParsingException {

        String[] fields = name.split("[.]");
        if (fields.length == 2 && (!fields[0].equals("null")))
            return name;
        if (fields.length > 2) 
            throw new ParsingException("Field " + name + " is not a valid field reference.");
        if (fields.length == 2)
            name = fields[1];
        if (name.equals("*")) return name;
        //now look for occurrences of name in all of the tables
        Iterator<LogicalScanNode> tableIt = tables.iterator();
        String tableName = null;
        while (tableIt.hasNext()) {
            LogicalScanNode table = tableIt.next();
            try {
                TupleDesc td = Database.getCatalog().getDbFile(table.t).getTupleDesc();
//                int id = 
                  td.fieldNameToIndex(name);
                if (tableName == null) {
                    tableName = table.alias;
                } else {
                    throw new ParsingException("Field " + name + " appears in multiple tables; disambiguate by referring to it as tablename." + name);
                }
            } catch (NoSuchElementException e) {
                //ignore
            }
        }
        if (tableName != null)
            return tableName + "." + name;
        else
            throw new ParsingException("Field " + name + " does not appear in any tables.");

    }

    /** Convert the aggregate operator name s into an Aggregator.op operation.
     *  @throws ParsingException if s is not a valid operator name 
     */
    static Aggregator.Op getAggOp(String s) throws ParsingException {
        s = s.toUpperCase();
        if (s.equals("AVG")) return Aggregator.Op.AVG;
        if (s.equals("SUM")) return Aggregator.Op.SUM;
        if (s.equals("COUNT")) return Aggregator.Op.COUNT;
        if (s.equals("MIN")) return Aggregator.Op.MIN;
        if (s.equals("MAX")) return Aggregator.Op.MAX;
        throw new ParsingException("Unknown predicate " + s);
    }

    /** Convert this LogicalPlan into a physicalPlan represented by a {@link DbIterator}.  Attempts to
     *   find the optimal plan by using {@link JoinOptimizer#orderJoins} to order the joins in the plan.
     *  @param t The transaction that the returned DbIterator will run as a part of
     *  @param baseTableStats a HashMap providing a {@link TableStats}
     *    object for each table used in the LogicalPlan.  This should
     *    have one entry for each table referenced by the plan, not one
     *    entry for each table alias (so a table t aliases as t1 and
     *    t2 would have just one entry with key 't' in this HashMap).
     *  @param explain flag indicating whether output visualizing the physical
     *    query plan should be given.
     *  @throws ParsingException if the logical plan is not valid
     *  @return A DbIterator representing this plan.
     */ 
    public DbIterator physicalPlan(TransactionId t, Map<String,TableStats> baseTableStats, boolean explain) throws ParsingException {
        Iterator<LogicalScanNode> tableIt = tables.iterator();
        HashMap<String,String> equivMap = new HashMap<String,String>();
        HashMap<String,Double> filterSelectivities = new HashMap<String, Double>();
        HashMap<String,TableStats> statsMap = new HashMap<String,TableStats>();

        while (tableIt.hasNext()) {
            LogicalScanNode table = tableIt.next();
            SeqScan ss = null;
            try {
                 ss = new SeqScan(t, Database.getCatalog().getDbFile(table.t).getId(), table.alias);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown table " + table.t);
            }
            
            subplanMap.put(table.alias,ss);
            String baseTableName = Database.getCatalog().getTableName(table.t);
            statsMap.put(baseTableName, baseTableStats.get(baseTableName));
            filterSelectivities.put(table.alias, 1.0);

        }

        Iterator<LogicalFilterNode> filterIt = filters.iterator();        
        while (filterIt.hasNext()) {
            LogicalFilterNode lf = filterIt.next();
            DbIterator subplan = subplanMap.get(lf.tableAlias);
            if (subplan == null) {
                throw new ParsingException("Unknown table in WHERE clause " + lf.tableAlias);
            }

            Field f;
            Type ftyp;
            TupleDesc td = subplanMap.get(lf.tableAlias).getTupleDesc();
            
            try {//td.fieldNameToIndex(disambiguateName(lf.fieldPureName))
                ftyp = td.getFieldType(td.fieldNameToIndex(lf.fieldQuantifiedName));
            } catch (java.util.NoSuchElementException e) {
                throw new ParsingException("Unknown field in filter expression " + lf.fieldQuantifiedName);
            }
            if (ftyp == Type.INT_TYPE)
                f = new IntField(new Integer(lf.c).intValue());
            else
                f = new StringField(lf.c, Type.STRING_LEN);

            Predicate p = null;
            try {
                p = new Predicate(subplan.getTupleDesc().fieldNameToIndex(lf.fieldQuantifiedName), lf.p,f);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field " + lf.fieldQuantifiedName);
            }
            subplanMap.put(lf.tableAlias, new Filter(p, subplan));

            TableStats s = statsMap.get(Database.getCatalog().getTableName(this.getTableId(lf.tableAlias)));
            
            double sel= s.estimateSelectivity(subplan.getTupleDesc().fieldNameToIndex(lf.fieldQuantifiedName), lf.p, f);
            filterSelectivities.put(lf.tableAlias, filterSelectivities.get(lf.tableAlias) * sel);

            //s.addSelectivityFactor(estimateFilterSelectivity(lf,statsMap));
        }
        
        JoinOptimizer jo = new JoinOptimizer(this,joins);

        joins = jo.orderJoins(statsMap,filterSelectivities,explain);

        Iterator<LogicalJoinNode> joinIt = joins.iterator();
        while (joinIt.hasNext()) {
            LogicalJoinNode lj = joinIt.next();
            DbIterator plan1;
            DbIterator plan2;
            boolean isSubqueryJoin = lj instanceof LogicalSubplanJoinNode;
            String t1name, t2name;

            if (equivMap.get(lj.t1Alias)!=null)
                t1name = equivMap.get(lj.t1Alias);
            else
                t1name = lj.t1Alias;

            if (equivMap.get(lj.t2Alias)!=null)
                t2name = equivMap.get(lj.t2Alias);
            else
                t2name = lj.t2Alias;

            plan1 = subplanMap.get(t1name);

            if (isSubqueryJoin) {
                plan2 = ((LogicalSubplanJoinNode)lj).subPlan;
                if (plan2 == null) 
                    throw new ParsingException("Invalid subquery.");
            } else { 
                plan2 = subplanMap.get(t2name);
            }
            
            if (plan1 == null)
                throw new ParsingException("Unknown table in WHERE clause " + lj.t1Alias);
            if (plan2 == null)
                throw new ParsingException("Unknown table in WHERE clause " + lj.t2Alias);
            
            DbIterator j;
            j = jo.instantiateJoin(lj,plan1,plan2);
            subplanMap.put(t1name, j);

            if (!isSubqueryJoin) {
                subplanMap.remove(t2name);
                equivMap.put(t2name,t1name);  //keep track of the fact that this new node contains both tables
                    //make sure anything that was equiv to lj.t2 (which we are just removed) is
                    // marked as equiv to lj.t1 (which we are replacing lj.t2 with.)
                    for (java.util.Map.Entry<String, String> s: equivMap.entrySet()) {
                        String val = s.getValue();
                        if (val.equals(t2name)) {
                            s.setValue(t1name);
                        }
                    }
                    
                // subplanMap.put(lj.t2, j);
            }
            
        }

        if (subplanMap.size() > 1) {
            throw new ParsingException("Query does not include join expressions joining all nodes!");
        }
        
        DbIterator node =  (DbIterator)(subplanMap.entrySet().iterator().next().getValue());

        //walk the select list, to determine order in which to project output fields
        ArrayList<Integer> outFields = new ArrayList<Integer>();
        ArrayList<Type> outTypes = new ArrayList<Type>();
        for (int i = 0; i < selectList.size(); i++) {
            LogicalSelectListNode si = selectList.elementAt(i);
            if (si.aggOp != null) {
                outFields.add(groupByField!=null?1:0);
                TupleDesc td = node.getTupleDesc();
//                int  id;
                try {
//                    id = 
                    td.fieldNameToIndex(si.fname);
                } catch (NoSuchElementException e) {
                    throw new ParsingException("Unknown field " +  si.fname + " in SELECT list");
                }
                outTypes.add(Type.INT_TYPE);  //the type of all aggregate functions is INT

            } else if (hasAgg) {
                    if (groupByField == null) {
                        throw new ParsingException("Field " + si.fname + " does not appear in GROUP BY list");
                    }
                    outFields.add(0);
                    TupleDesc td = node.getTupleDesc();
                    int  id;
                    try {
                        id = td.fieldNameToIndex(groupByField);
                    } catch (NoSuchElementException e) {
                        throw new ParsingException("Unknown field " +  groupByField + " in GROUP BY statement");
                    }
                    outTypes.add(td.getFieldType(id));
            } else if (si.fname.equals("null.*")) {
                    TupleDesc td = node.getTupleDesc();
                    for ( i = 0; i < td.numFields(); i++) {
                        outFields.add(i);
                        outTypes.add(td.getFieldType(i));
                    }
            } else  {
                    TupleDesc td = node.getTupleDesc();
                    int id;
                    try {
                        id = td.fieldNameToIndex(si.fname);
                    } catch (NoSuchElementException e) {
                        throw new ParsingException("Unknown field " +  si.fname + " in SELECT list");
                    }
                    outFields.add(id);
                    outTypes.add(td.getFieldType(id));

                }
        }

        if (hasAgg) {
            TupleDesc td = node.getTupleDesc();
            Aggregate aggNode;
            try {
                aggNode = new Aggregate(node,
                                        td.fieldNameToIndex(aggField),
                                        groupByField == null?Aggregator.NO_GROUPING:td.fieldNameToIndex(groupByField),
                                getAggOp(aggOp));
            } catch (NoSuchElementException e) {
                throw new simpledb.ParsingException(e);
            } catch (IllegalArgumentException e) {
                throw new simpledb.ParsingException(e);
            }
            node = aggNode;
        }

        if (hasOrderBy) {
            node = new OrderBy(node.getTupleDesc().fieldNameToIndex(oByField), oByAsc, node);
        }

        return new Project(outFields, outTypes, node);
    }

    public static void main(String argv[]) {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };

        TupleDesc td = new TupleDesc(types, names);
        TableStats ts;
        HashMap<String, TableStats> tableMap = new HashMap<String,TableStats>();

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");
        ts = new TableStats(table1.getId(), 1);
        tableMap.put("t1", ts);

        TransactionId tid = new TransactionId();

        LogicalPlan lp = new LogicalPlan();
        
        lp.addScan(table1.getId(), "t1");

        try {
            lp.addFilter("t1.field0", Predicate.Op.GREATER_THAN, "1");
        } catch (Exception e) {
        }

        /*
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                                new Predicate(0,
                                Predicate.Op.GREATER_THAN, new IntField(1)),  ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);
        */
        DbIterator j = null;
        try {
            j = lp.physicalPlan(tid,tableMap, false);
        } catch (ParsingException e) {
            e.printStackTrace();
            System.exit(0);
        }
        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }
       
    }

}