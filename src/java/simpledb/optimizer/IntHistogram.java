package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int min;
    private int max;
    private int ntups;
    private double width;

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
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = Math.max(1.0, (max - min + 1.0) / buckets);
//        this.width = (max - min + 1.0) / buckets;
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v >= min && v <= max && getIndex(v) != -1) {
            buckets[getIndex(v)] ++;
            ntups ++;
        }
    }

    private int getIndex(int v) {
        int index = (int) ((v - min) / width);
        if(index < 0 || index >= buckets.length){
            return -1;
        }
        return index;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case EQUALS:
            {
                int idx = getIndex(v);
                if(idx == -1) return 0.0;
                return (double) buckets[idx] / width / ntups;
            }
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN:
            {
                if(v >= max) return 0;
                if(v < min) return 1;
                int idx = getIndex(v);
                double sum = 0;
                for(int i = idx + 1; i < buckets.length; i ++)
                    sum += buckets[i];
                sum += (min + (idx + 1) * width - v - 1) * (buckets[idx] / width);
                return sum / ntups;
            }
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.EQUALS, v) + estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case LESS_THAN:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
            case LESS_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            default:
                throw new UnsupportedOperationException("Op is illegal");

        }
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        double sum = 0;
        for (int bucket : buckets) {
            sum += bucket;
        }
        return sum / ntups / buckets.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + Arrays.toString(buckets) +
                ", min=" + min +
                ", max=" + max +
                ", ntups=" + ntups +
                ", width=" + width +
                '}';
    }
}
