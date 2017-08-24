package simpledb;

//import java.util.HashMap;
import java.util.Map;

/**
 * A utility class, which computes the estimated cardinalities of an operator
 * tree.
 * 
 * All methods have been fully provided. No extra codes are required.
 */
public class OperatorCardinality {

    /**
     * 
     * @param tableAliasToId
     *            table alias to table id mapping
     * @param tableStats
     *            table statistics
     * */
    public static boolean updateOperatorCardinality(Operator o,
            Map<String, Integer> tableAliasToId,
            Map<String, TableStats> tableStats) {
        if (o instanceof Filter) {
            return updateFilterCardinality((Filter) o, tableAliasToId,
                    tableStats);
        } else if (o instanceof Join) {
            return updateJoinCardinality((Join) o, tableAliasToId, tableStats);
        } else if (o instanceof HashEquiJoin) {
            return updateHashEquiJoinCardinality((HashEquiJoin) o,
                    tableAliasToId, tableStats);
        } else if (o instanceof Aggregate) {
            return updateAggregateCardinality((Aggregate) o, tableAliasToId,
                    tableStats);
        } else {
            DbIterator[] children = o.getChildren();
            int childC = 1;
            boolean hasJoinPK = false;
            if (children.length > 0 && children[0] != null) {
                if (children[0] instanceof Operator) {
                    hasJoinPK = updateOperatorCardinality(
                            (Operator) children[0], tableAliasToId, tableStats);
                    childC = ((Operator) children[0]).getEstimatedCardinality();
                } else if (children[0] instanceof SeqScan) {
                    childC = tableStats.get(
                            ((SeqScan) children[0]).getTableName())
                            .estimateTableCardinality(1.0);
                }
            }
            o.setEstimatedCardinality(childC);
            return hasJoinPK;
        }
    }

    private static boolean updateFilterCardinality(Filter f,
            Map<String, Integer> tableAliasToId,
            Map<String, TableStats> tableStats) {
        DbIterator child = f.getChildren()[0];
        Predicate pred = f.getPredicate();
        String[] tmp = child.getTupleDesc().getFieldName(pred.getField())
                .split("[.]");
        String tableAlias = tmp[0];
        String pureFieldName = tmp[1];
        Integer tableId = tableAliasToId.get(tableAlias);
        double selectivity = 1.0;
        if (tableId != null) {
            selectivity = tableStats.get(
                    Database.getCatalog().getTableName(tableId))
                    .estimateSelectivity(
                            Database.getCatalog().getTupleDesc(tableId)
                                    .fieldNameToIndex(pureFieldName),
                            pred.getOp(), pred.getOperand());
            if (child instanceof Operator) {
                Operator oChild = (Operator) child;
                boolean hasJoinPK = updateOperatorCardinality(oChild,
                        tableAliasToId, tableStats);
                f.setEstimatedCardinality((int) (oChild
                        .getEstimatedCardinality() * selectivity) + 1);
                return hasJoinPK;
            } else if (child instanceof SeqScan) {
                f.setEstimatedCardinality((int) (tableStats.get(
                        ((SeqScan) child).getTableName())
                        .estimateTableCardinality(1.0) * selectivity) + 1);
                return false;
            }
        }
        f.setEstimatedCardinality(1);
        return false;
    }

    private static boolean updateJoinCardinality(Join j,
            Map<String, Integer> tableAliasToId,
            Map<String, TableStats> tableStats) {

        DbIterator[] children = j.getChildren();
        DbIterator child1 = children[0];
        DbIterator child2 = children[1];
        int child1Card = 1;
        int child2Card = 1;

        String[] tmp1 = j.getJoinField1Name().split("[.]");
        String tableAlias1 = tmp1[0];
        String pureFieldName1 = tmp1[1];

        String[] tmp2 = j.getJoinField2Name().split("[.]");
        String tableAlias2 = tmp2[0];
        String pureFieldName2 = tmp2[1];

        boolean child1HasJoinPK = Database.getCatalog()
                .getPrimaryKey(tableAliasToId.get(tableAlias1))
                .equals(pureFieldName1);
        boolean child2HasJoinPK = Database.getCatalog()
                .getPrimaryKey(tableAliasToId.get(tableAlias2))
                .equals(pureFieldName2);

        if (child1 instanceof Operator) {
            Operator child1O = (Operator) child1;
            boolean pk = updateOperatorCardinality(child1O, tableAliasToId,
                    tableStats);
            child1HasJoinPK = pk || child1HasJoinPK;
            child1Card = child1O.getEstimatedCardinality();
            child1Card = child1Card > 0 ? child1Card : 1;
        } else if (child1 instanceof SeqScan) {
            child1Card = (int) (tableStats.get(((SeqScan) child1)
                    .getTableName()).estimateTableCardinality(1.0));
        }

        if (child2 instanceof Operator) {
            Operator child2O = (Operator) child2;
            boolean pk = updateOperatorCardinality(child2O, tableAliasToId,
                    tableStats);
            child2HasJoinPK = pk || child2HasJoinPK;
            child2Card = child2O.getEstimatedCardinality();
            child2Card = child2Card > 0 ? child2Card : 1;
        } else if (child2 instanceof SeqScan) {
            child2Card = (int) (tableStats.get(((SeqScan) child2)
                    .getTableName()).estimateTableCardinality(1.0));
        }

        j.setEstimatedCardinality(JoinOptimizer.estimateTableJoinCardinality(j
                .getJoinPredicate().getOperator(), tableAlias1, tableAlias2,
                pureFieldName1, pureFieldName2, child1Card, child2Card,
                child1HasJoinPK, child2HasJoinPK, tableStats, tableAliasToId));
        return child1HasJoinPK || child2HasJoinPK;
    }

    private static boolean updateHashEquiJoinCardinality(HashEquiJoin j,
            Map<String, Integer> tableAliasToId,
            Map<String, TableStats> tableStats) {

        DbIterator[] children = j.getChildren();
        DbIterator child1 = children[0];
        DbIterator child2 = children[1];
        int child1Card = 1;
        int child2Card = 1;

        String[] tmp1 = j.getJoinField1Name().split("[.]");
        String tableAlias1 = tmp1[0];
        String pureFieldName1 = tmp1[1];
        String[] tmp2 = j.getJoinField2Name().split("[.]");
        String tableAlias2 = tmp2[0];
        String pureFieldName2 = tmp2[1];

        boolean child1HasJoinPK = Database.getCatalog()
                .getPrimaryKey(tableAliasToId.get(tableAlias1))
                .equals(pureFieldName1);
        ;
        boolean child2HasJoinPK = Database.getCatalog()
                .getPrimaryKey(tableAliasToId.get(tableAlias2))
                .equals(pureFieldName2);
        ;

        if (child1 instanceof Operator) {
            Operator child1O = (Operator) child1;
            boolean pk = updateOperatorCardinality(child1O, tableAliasToId,
                    tableStats);
            child1HasJoinPK = pk || child1HasJoinPK;
            child1Card = child1O.getEstimatedCardinality();
            child1Card = child1Card > 0 ? child1Card : 1;
        } else if (child1 instanceof SeqScan) {
            child1Card = (int) (tableStats.get(((SeqScan) child1)
                    .getTableName()).estimateTableCardinality(1.0));
        }

        if (child2 instanceof Operator) {
            Operator child2O = (Operator) child2;
            boolean pk = updateOperatorCardinality(child2O, tableAliasToId,
                    tableStats);
            child2HasJoinPK = pk || child2HasJoinPK;
            child2Card = child2O.getEstimatedCardinality();
            child2Card = child2Card > 0 ? child2Card : 1;
        } else if (child2 instanceof SeqScan) {
            child2Card = (int) (tableStats.get(((SeqScan) child2)
                    .getTableName()).estimateTableCardinality(1.0));
        }

        j.setEstimatedCardinality(JoinOptimizer.estimateTableJoinCardinality(j
                .getJoinPredicate().getOperator(), tableAlias1, tableAlias2,
                pureFieldName1, pureFieldName2, child1Card, child2Card,
                child1HasJoinPK, child2HasJoinPK, tableStats, tableAliasToId));
        return child1HasJoinPK || child2HasJoinPK;
    }

    private static boolean updateAggregateCardinality(Aggregate a,
            Map<String, Integer> tableAliasToId,
            Map<String, TableStats> tableStats) {
        DbIterator child = a.getChildren()[0];
        int childCard = 1;
        boolean hasJoinPK = false;
        if (child instanceof Operator) {
            Operator oChild = (Operator) child;
            hasJoinPK = updateOperatorCardinality(oChild, tableAliasToId,
                    tableStats);
            childCard = oChild.getEstimatedCardinality();
        }

        if (a.groupField() == Aggregator.NO_GROUPING) {
            a.setEstimatedCardinality(1);
            return hasJoinPK;
        }

        if (child instanceof SeqScan) {
            childCard = (int) (tableStats.get(((SeqScan) child).getTableName())
                    .estimateTableCardinality(1.0));
        }

        String[] tmp = a.groupFieldName().split("[.]");
        String tableAlias = tmp[0];
        String pureFieldName = tmp[1];
        Integer tableId = tableAliasToId.get(tableAlias);

        double groupFieldAvgSelectivity = 1.0;
        if (tableId != null) {
            groupFieldAvgSelectivity = tableStats.get(
                    Database.getCatalog().getTableName(tableId))
                    .avgSelectivity(
                            Database.getCatalog().getTupleDesc(tableId)
                                    .fieldNameToIndex(pureFieldName),
                            Predicate.Op.EQUALS);
            a.setEstimatedCardinality((int) (Math.min(childCard,
                    1.0 / groupFieldAvgSelectivity)));
            return hasJoinPK;
        }
        a.setEstimatedCardinality(childCard);
        return hasJoinPK;
    }
}
