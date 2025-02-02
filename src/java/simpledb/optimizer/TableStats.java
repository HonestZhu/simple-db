package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, IntHistogram> intHistogram;

    private ConcurrentHashMap<Integer, StringHistogram> stringHistogram;

    private int totalTuples;

    private int ioCostPerPage;

    private HeapFile file;

    private TupleDesc tupleDesc;
    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.totalTuples = 0;
        this.intHistogram = new ConcurrentHashMap<>();
        this.stringHistogram = new ConcurrentHashMap<>();
        this.ioCostPerPage = ioCostPerPage;

        this.file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDesc = file.getTupleDesc();
        HashMap<Integer, Integer> mins = new HashMap<>();
        HashMap<Integer, Integer> maxs = new HashMap<>();

        Transaction transaction = new Transaction();
        transaction.start();
        DbFileIterator child = file.iterator(transaction.getId());

        try {
            child.open();
            while(child.hasNext()) {
                this.totalTuples ++;
                Tuple tuple = child.next();
                for(int i = 0; i < tupleDesc.numFields(); i ++) {
                    if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntField field = (IntField) tuple.getField(i);
                        mins.put(i, Math.min(field.getValue(), mins.getOrDefault(i, Integer.MAX_VALUE)));
                        maxs.put(i, Math.max(field.getValue(), maxs.getOrDefault(i, Integer.MIN_VALUE)));
                    } else if(tupleDesc.getFieldType(i).equals(Type.STRING_TYPE)) {
                        StringHistogram histogram = this.stringHistogram.getOrDefault(i, new StringHistogram(NUM_HIST_BINS));
                        StringField field = (StringField) tuple.getField(i);
                        histogram.addValue(field.getValue());
                        this.stringHistogram.put(i, histogram);
                    }
                }
            }
            for(int i = 0; i < tupleDesc.numFields(); i ++) {
                if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                    this.intHistogram.put(i, new IntHistogram(NUM_HIST_BINS, mins.get(i), maxs.get(i)));
                }
            }

            child.rewind();
            while(child.hasNext()) {
                Tuple tuple = child.next();
                for(int i = 0; i < tupleDesc.numFields(); i ++) {
                    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntField f = (IntField) tuple.getField(i);
                        IntHistogram intHis = this.intHistogram.get(i);
                        if (intHis == null) throw new IllegalArgumentException("Fail to get IntHistogram.");
                        intHis.addValue(f.getValue());
                        this.intHistogram.put(i, intHis);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            child.close();
            try {
                transaction.commit();
            } catch (IOException e) {
                System.out.println("事务提交失败！！");
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return file.numPages() * ioCostPerPage * 2;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) ( totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (tupleDesc.getFieldType(field).equals(Type.INT_TYPE)) {
            return intHistogram.get(field).avgSelectivity();
        }else if(tupleDesc.getFieldType(field).equals(Type.STRING_TYPE)){
            return stringHistogram.get(field).avgSelectivity();
        }
        return -1.00;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (tupleDesc.getFieldType(field).equals(Type.INT_TYPE)) {
            IntField intField = (IntField) constant;
            return intHistogram.get(field).estimateSelectivity(op,intField.getValue());
        } else if(tupleDesc.getFieldType(field).equals(Type.STRING_TYPE)){
            StringField stringField = (StringField) constant;
            return stringHistogram.get(field).estimateSelectivity(op,stringField.getValue());
        }
        return -1.00;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return totalTuples;
    }

}
