package simpledb;

/** A class to represent a fixed-width histogram over a single String-based field.
 */
public class StringHistogram {
    IntHistogram hist;

    /** Create a new StringHistogram with a specified number of buckets.
        <p>
        Our implementation is written in terms of an IntHistogram by converting
        each String to an integer.
        @param buckets the number of buckets */
    public StringHistogram(int buckets) {
        hist = new IntHistogram(buckets, minVal(), maxVal());
    }

    /** Convert a string to an integer, with the property that 
        if the return value(s1) < return value(s2), then s1 < s2
    */
    private int stringToInt(String s) {
        int i ;
        int v = 0;
        for (i = 3; i >= 0;i--) {
            if (s.length() > 3-i) {
                int ci = (int)s.charAt(3-i);
                v += (ci) << (i * 8);
            } 
        }
        
        // XXX: hack to avoid getting wrong results for
        // strings which don't output in the range min to max
        if (!(s.equals("") || s.equals("zzzz"))) {
	        if (v < minVal()) {
	        	v = minVal();
	        } 
	        
	        if (v > maxVal()) {
	        	v = maxVal();
	        }
        }
        
        return v;
    }

    /** @return the maximum value indexed by the histogram */
    int maxVal() {
        return stringToInt("zzzz");
    }

    /** @return the minimum value indexed by the histogram */
    int minVal() {
        return stringToInt("");
    }

    /** Add a new value to thte histogram */
    public void addValue(String s) {
        int val = stringToInt(s);
        hist.addValue(val);
    }

    /** Estimate the selectivity (as a double between 0 and 1) of the specified predicate over the specified string 
        @param op The operation being applied
        @param s The string to apply op to 
    */
    public double estimateSelectivity(Predicate.Op op, String s) {
        int val = stringToInt(s);
        return hist.estimateSelectivity(op, val);
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return hist.avgSelectivity();
    }
}