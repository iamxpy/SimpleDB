package simpledb;

import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.util.*;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    LogicalPlan p;
    Vector<LogicalJoinNode> joins;

    /**
     * Constructor
     *
     * @param p     the logical plan being optimized
     * @param joins the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, Vector<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best iterator for computing a given logical join, given the
     * specified statistics, and the provided left and right subplans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because DbIterator's don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1
     *
     * @param lj    The join being considered
     * @param plan1 The left join node's child
     * @param plan2 The right join node's child
     */
    public static DbIterator instantiateJoin(LogicalJoinNode lj,
                                             DbIterator plan1, DbIterator plan2) throws ParsingException {

        int t1id, t2id;
        DbIterator j;

        try {
            t1id = plan1.getTupleDesc().fieldNameToIndex(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().fieldNameToIndex(
                        lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field "
                        + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);

        j = new Join(p, plan1, plan2);

        return j;

    }

    /**
     * Estimate the cost of a join.
     * <p>
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     *
     * @param j     A LogicalJoinNode representing the join operation being
     *              performed.
     * @param card1 Estimated cardinality of the left-hand side of the query
     * @param card2 Estimated cardinality of the right-hand side of the query
     * @param cost1 Estimated cost of one full scan of the table on the left-hand
     *              side of the query
     * @param cost2 Estimated cost of one full scan of the table on the right-hand
     *              side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and
     * cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2,
                                   double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 4.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.

            //针对BlockNestedLoopJoin
            TupleDesc desc = p.getTupleDesc(j.t1Alias);
            int blockSize = Join.blockMemory / desc.getSize();
            int fullNum = card1 / blockSize;
            int left = (card1 - blockSize * fullNum) == 0 ? 0 : 1;
            int blockCard = fullNum + left;//得到左表被分成多少个缓冲区
            double cost = cost1 + blockCard * cost2 + (double) card1 * (double) card2;
            return cost;
        }
    }


    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     *
     * @param j      A LogicalJoinNode representing the join operation being
     *               performed.
     * @param card1  Cardinality of the left-hand table in the join
     * @param card2  Cardinality of the right-hand table in the join
     * @param t1pkey Is the left-hand table a primary-key table?
     * @param t2pkey Is the right-hand table a primary-key table?
     * @param stats  The table stats, referenced by table names, not alias
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
                                       boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 4.
            return card1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias,
                    j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey,
                    stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
                                                   String table1Alias, String table2Alias, String field1PureName,
                                                   String field2PureName, int card1, int card2, boolean t1pkey,
                                                   boolean t2pkey, Map<String, TableStats> stats,
                                                   Map<String, Integer> tableAliasToId) {
        int card;
        // some code goes here
        int smallerSize = card1 < card2 ? card1 : card2;
        int biggerSize = card1 > card2 ? card1 : card2;
        switch (joinOp) {
            case EQUALS:
                if (t1pkey && t2pkey) {
                    card = smallerSize;
                } else if (t1pkey && !t2pkey) {
                    card = card2;
                } else if (!t1pkey && t2pkey) {
                    card = card1;
                } else {
                    card = biggerSize;
                }
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                card = (int) (card1 * card2 * 0.3);
                break;
            default:
                card = card1 * card2;
        }
        return card <= 0 ? 1 : card;
    }

    /**
     * Helper method to enumerate all of the subsets of a given size of a
     * specified vector.
     *
     * @param v    The vector whose subsets are desired
     * @param size The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */
    @SuppressWarnings("unchecked")
    public <T> Set<Set<T>> enumerateSubsets(Vector<T> v, int size) {
        Set<Set<T>> els = new HashSet<Set<T>>();
        els.add(new HashSet<T>());
        // Iterator<Set> it;
        // long start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            Set<Set<T>> newels = new HashSet<Set<T>>();
            for (Set<T> s : els) {
                for (T t : v) {
                    Set<T> news = (Set<T>) (((HashSet<T>) s).clone());
                    if (news.add(t))
                        newels.add(news);
                }
            }
            els = newels;
        }

        return els;

    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * PS4 for hints on how this should be implemented.
     *
     * @param stats               Statistics for each table involved in the join, referenced by
     *                            base table names, not alias
     * @param filterSelectivities Selectivities of the filter predicates on each table in the
     *                            join, referenced by table alias (if no alias, the base table
     *                            name)
     * @param explain             Indicates whether your code should explain its query plan or
     *                            simply execute it
     * @return A Vector<LogicalJoinNode> that stores joins in the left-deep
     * order in which they should be executed.
     * @throws ParsingException when stats or filter selectivities is missing a table in the
     *                          join, or or when another internal error occurs
     */
    // TODO: 17-9-4 figure why problems occur when Qurey is "select * from ...."
    public Vector<LogicalJoinNode> orderJoins(
            HashMap<String, TableStats> stats,
            HashMap<String, Double> filterSelectivities, boolean explain)
            throws ParsingException {

        // some code goes here
        //Replace the following
        // 1. j = set of join nodes
        // 2. for (i in 1...|j|):  // First find best plan for single join, then for two joins, etc.
        // 3.     for s in {all length i subsets of j} // Looking at a concrete subset of joins
        // 4.       bestPlan = {}  // We want to find the best plan for this concrete subset
        // 5.       for s' in {all length i-1 subsets of s}
        // 6.            subplan = optjoin(s')  // Look-up in the cache the best query plan for s but with one relation missing
        // 7.            plan = best way to join (s-s') to subplan // Now find the best plan to extend s' by one join to get s
        // 8.            if (cost(plan) < cost(bestPlan))
        // 9.               bestPlan = plan // Update the best plan for computing s
        // 10.      optjoin(s) = bestPlan
        // 11. return optjoin(j)

        int numJoinNodes = joins.size();
        PlanCache pc = new PlanCache();
        Set<LogicalJoinNode> wholeSet = null;
        for (int i = 1; i <= numJoinNodes; i++) {
            Set<Set<LogicalJoinNode>> setOfSubset = this.enumerateSubsets(this.joins, i);
            for (Set<LogicalJoinNode> s : setOfSubset) {
                if (s.size() == numJoinNodes) {
                    wholeSet = s;//将join节点的全集保存下来最后用
                }
                Double bestCostSofar = Double.MAX_VALUE;
                CostCard bestPlan = new CostCard();
                for (LogicalJoinNode toRemove : s) {
                    CostCard plan = computeCostAndCardOfSubplan(stats, filterSelectivities, toRemove, s, bestCostSofar, pc);
                    if (plan != null) {
                        bestCostSofar = plan.cost;
                        bestPlan = plan;
                    }
                }
                if (bestPlan.plan != null) {
                    pc.addPlan(s, bestPlan.cost, bestPlan.card, bestPlan.plan);
                }
            }
        }
        return pc.getOrder(wholeSet);
    }


    // ===================== Private Methods =================================


    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats               table stats for all of the tables, referenced by table names
     *                            rather than alias (see {@link #orderJoins})
     * @param filterSelectivities the selectivities of the filters over each of the tables
     *                            (where tables are indentified by their alias or name if no
     *                            alias is given)
     * @param joinToRemove        the join to remove from joinSet
     * @param joinSet             the set of joins being considered
     * @param bestCostSoFar       the best way to join joinSet so far (minimum of previous
     *                            invocations of computeCostAndCardOfSubplan for this joinSet,
     *                            from returned CostCard)
     * @param pc                  the PlanCache for this join; should have subplans for all
     *                            plans of size joinSet.size()-1
     * @return A {@link CostCard} objects desribing the cost, cardinality,
     * optimal subplan
     * @throws ParsingException when stats, filterSelectivities, or pc object is missing
     *                          tables involved in join
     */
    @SuppressWarnings("unchecked")
    private CostCard computeCostAndCardOfSubplan(
            HashMap<String, TableStats> stats,
            HashMap<String, Double> filterSelectivities,
            LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet,
            double bestCostSoFar, PlanCache pc) throws ParsingException {

        LogicalJoinNode j = joinToRemove;

        Vector<LogicalJoinNode> prevBest;

        if (this.p.getTableId(j.t1Alias) == null)
            throw new ParsingException("Unknown table " + j.t1Alias);
        if (this.p.getTableId(j.t2Alias) == null)
            throw new ParsingException("Unknown table " + j.t2Alias);

        String table1Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias;
        String table2Alias = j.t2Alias;

        Set<LogicalJoinNode> news = (Set<LogicalJoinNode>) ((HashSet<LogicalJoinNode>) joinSet)
                .clone();
        news.remove(j);

        double t1cost, t2cost, cost = 0;
        int t1card, t2card;
        boolean leftPkey, rightPkey;
        boolean leftInPreBest = false, rightInPreBest = false;

        if (news.isEmpty()) { // base case -- both are base relations
            //移除一个join之后就为null，说明传入的joinSet只含有一个join
            prevBest = new Vector<>();
            t1cost = stats.get(table1Name).estimateScanCost();
            t1card = stats.get(table1Name).estimateTableCardinality(
                    filterSelectivities.get(j.t1Alias));
            leftPkey = isPkey(j.t1Alias, j.f1PureName);

            t2cost = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateScanCost();
            t2card = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateTableCardinality(
                            filterSelectivities.get(j.t2Alias));
            rightPkey = table2Alias != null && isPkey(table2Alias, j.f2PureName);
        } else {//移除一个join后不为空则joinSet里面的join大于等于2
            // news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news);//从plan缓存中获取left-deep-tree的最佳plan

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest == null) {
                //如果pc中没有对应的最佳plan，说明这个左树没有最佳plan，就不用继续为其加一个join节点了
                return null;
            }

            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            // estimate cost of right subtree
            leftInPreBest = doesJoin(prevBest, table1Alias);
            rightInPreBest = doesJoin(prevBest, table2Alias);
            //为了保证生成的是left-deep-tree，必须保证新加入的join节点的左表或右表在左树中存在
            //即保证新加入的join能按照left-deep-tree的形式与左树拼接起来
            if (leftInPreBest || rightInPreBest) {//
                t1cost = prevBestCost; // left side just has cost of whatever
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateTableCardinality(
                                filterSelectivities.get(j.t2Alias));
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName);
            } else {//无法生成left-deep-tree则返回null
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return null;
            }
        }

        //计算得到cost
        //注意在后面left-deep-tree成型之后，新加进来的join的顺序其实是固定的，即下面的两种else if的情况
        if (prevBest.size() == 0) {//如果树还未成型，允许对新加的join调整其inner和outer
            double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);
            cost = cost1;
            LogicalJoinNode j2 = j.swapInnerOuter();
            double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
            if (cost2 < cost1) {//如果交换inner和outer之后代价更小
                j = j2;
                cost = cost2;
                boolean tmp = rightPkey;
                rightPkey = leftPkey;
                leftPkey = tmp;
                int tmp1 = t2card;
                t2card = t1card;
                t1card = tmp1;
            }
        } else if (leftInPreBest) {//如果树已经成型，而且新加进来的join的左表在树中
            cost = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);
        } else if (rightInPreBest) {//如果树已经成型，而且新加进来的join的右表在树中
            LogicalJoinNode j2 = j.swapInnerOuter();
            cost = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
            boolean tmp;
            j = j2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
            int tmp1 = t2card;
            t2card = t1card;
            t1card = tmp1;
        }
        if (cost >= bestCostSoFar)
            return null;
        CostCard cc = new CostCard();

        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey,
                rightPkey, stats);
        cc.cost = cost;
        cc.plan = (Vector<LogicalJoinNode>) prevBest.clone();
        cc.plan.addElement(j); // prevbest is left -- add new join to end
        return cc;
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    private boolean doesJoin(Vector<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table)
                    || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     *
     * @param tableAlias The alias of the table in the query
     * @param field      The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    private boolean hasPkey(Vector<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName)
                    || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     *
     * @param js            the join plan to visualize
     * @param pc            the PlanCache accumulated whild building the optimal plan
     * @param stats         table statistics for base tables
     * @param selectivities the selectivities of the filters over each of the tables
     *                      (where tables are indentified by their alias or name if no
     *                      alias is given)
     */
    private void printJoins(Vector<LogicalJoinNode> js, PlanCache pc,
                            HashMap<String, TableStats> stats,
                            HashMap<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        HashMap<String, DefaultMutableTreeNode> m = new HashMap<String, DefaultMutableTreeNode>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<LogicalJoinNode>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t2Alias));

            // Double c = pc.getCost(pathSoFar);
            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost ="
                    + pc.getCost(pathSoFar) + ", card = "
                    + pc.getCard(pathSoFar) + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                        selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                + " (Cost = "
                                + stats.get(table2Name)
                                .estimateScanCost()
                                + ", card = "
                                + stats.get(table2Name)
                                .estimateTableCardinality(
                                        selectivities
                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }

}
