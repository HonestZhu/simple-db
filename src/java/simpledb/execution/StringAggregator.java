package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.sql.SQLOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private boolean needGroup;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> groupMap;

    private ConcurrentHashMap<Field, Tuple> resMap;

    private TupleDesc aggDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.needGroup = gbfield >= 0;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new ConcurrentHashMap<>();

        if(needGroup) {
            aggDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggVal"});
        } else {
            aggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(needGroup && !gbfieldtype.equals(tup.getField(gbfield).getType())) {
            throw new IllegalArgumentException("Except groupType is: ["+ gbfieldtype + "] ,But given "+ tup.getField(gbfield).getType() + ".");
        }
        if(!(tup.getField(afield) instanceof IntField)) {
            if(!((tup.getField(afield) instanceof StringField) && what == Op.COUNT))
                throw new IllegalArgumentException("Except aggType is: [IntField] ,But given "+ tup.getField(afield).getType() + ".");
        }
        Field key;
        if(needGroup) key = tup.getField(gbfield);
        else key = new StringField("", 1);
        if(what == Op.SUM) {
            groupMap.put(key, groupMap.getOrDefault(key, 0) + ((IntField) tup.getField(afield)).getValue());
        } else if(what == Op.COUNT) {
            groupMap.put(key, groupMap.getOrDefault(key, 0) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            private Iterator<Map.Entry<Field, Integer>> iterator;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                iterator = StringAggregator.this.groupMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(iterator != null && iterator.hasNext()) return true;
                else return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Map.Entry<Field, Integer> next = iterator.next();
                Tuple tuple = new Tuple(StringAggregator.this.aggDesc);
                if(StringAggregator.this.needGroup) {
                    tuple.setField(0, next.getKey());
                    tuple.setField(1, new IntField(next.getValue()));
                }
                else tuple.setField(0, new IntField(next.getValue()));
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return StringAggregator.this.aggDesc;
            }

            @Override
            public void close() {
                iterator = null;
            }
        };
    }

}
