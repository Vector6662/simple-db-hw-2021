package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets, min, max, space;
    private int[] hist;
    private int tupleNums;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = Math.min(buckets, max - min + 1);
        this.min = min;
        this.max = max;
        this.space = (max - min + 1) / this.buckets;
        this.hist = new int[buckets];
        this.tupleNums = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = (v - min) / space;
        if (index>buckets - 1) index = buckets-1;
        hist[index]++;
        tupleNums++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (op.equals(Predicate.Op.GREATER_THAN)){ // >
            if (v < min) return 1.0;
            if (v >= max) return 0.0;

            int b_left = (v-min)/space;
            int h_b = hist[b_left];
            if (b_left == buckets -1) {
                return 1.0 *(max - v) / h_b * tupleNums;
            }
            int count=0;
            for (int i = b_left + 1; i < buckets; i++){
                count += hist[i];
            }
            double b_part = ((b_left + 1) * space + min - v -1) * 1.0 / space; // x分之space
            return (count + b_part * h_b) / tupleNums;
        }

        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) // >=
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) // <=
            return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        if (op.equals(Predicate.Op.EQUALS)) // =
            return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        if (op.equals(Predicate.Op.LESS_THAN)) // <
            return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
        if (op.equals(Predicate.Op.NOT_EQUALS)) // !=
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);

    	// some code goes here
        return -1.0;
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
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", space=" + space +
                ", hist=" + Arrays.toString(hist) +
                ", tupleNums=" + tupleNums +
                '}';
    }
}
