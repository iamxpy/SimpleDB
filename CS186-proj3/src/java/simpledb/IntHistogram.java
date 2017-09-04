package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 * 目前理解selectivity的定义：predicate应用在table后的结果集的tuple数量占原table的tuple数量的比例
 */
public class IntHistogram {

    private int min;
    private int max;
    private int buckets;
    public int ntups;
    private int width;
    private int[] histogram;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.min = min;
        this.max = max;
        this.buckets = buckets;
        double range = (double) (max - min + 1) / buckets;
        width = (int) Math.ceil(range);
        ntups = 0;
        histogram = new int[buckets];

        for (int i = 0; i < histogram.length; i++) {
            histogram[i] = 0;
        }
    }

    private int valueToIndex(int v) {
        if (v == max) {
            return buckets - 1;
        } else {
            return (v - min) / width;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = valueToIndex(v);
        histogram[index]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * 目前理解selectivity的定义：predicate应用在table后的结果集的tuple数量占原table的tuple数量的比例
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        int bucketIndex = valueToIndex(v);
        int height;
        int left = bucketIndex * width + min;
        int right = bucketIndex * width + min + width - 1;

        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    return 0.0;
                } else {
                    height = histogram[bucketIndex];
                    return (height * 1.0 / width) / ntups;
                }
            case GREATER_THAN:
                if (v < min) {
                    return 1.0;
                }
                if (v > max) {
                    return 0.0;
                }
                height = histogram[bucketIndex];
                double p1 = ((right - v) / width * 1.0) * (height * 1.0 / ntups);
                int allInRight = 0;
                for (int i = bucketIndex + 1; i < buckets; i++) {
                    allInRight += histogram[i];
                }
                double p2 = allInRight * 1.0 / ntups;
                return p1 + p2;
            case LESS_THAN:
                if (v < min) {
                    return 0.0;
                }
                if (v > max) {
                    return 1.0;
                }
                height = histogram[bucketIndex];
                double pp1 = ((v - left) / width * 1.0) * (height * 1.0 / ntups);
                int allInLeft = 0;
                for (int i = bucketIndex - 1; i >= 0; i--) {
                    allInLeft += histogram[i];
                }
                double pp2 = allInLeft * 1.0 / ntups;
                return pp1 + pp2;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case LIKE:
                //int应该不支持like才对，但是StringHistogram间接调用这个方法，那里应该支持。。。
                return avgSelectivity();
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                throw new RuntimeException("Should not reach hear");
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return "[to be implemented]";
    }
}
