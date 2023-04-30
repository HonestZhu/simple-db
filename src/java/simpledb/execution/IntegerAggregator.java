package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private boolean needGroup;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, GroupResut> groupMap;

    private ConcurrentHashMap<Field, Tuple> resMap;

    private TupleDesc aggDesc;

    private class GroupResut {
        public Integer total;
        public Integer count;

        public Op op;

        public GroupResut(Integer total, Op op) {
            this.total = total;
            this.count = 1;
            this.op = op;
        }

        public void update(Integer num) {
            switch (op) {
                case AVG:
                    total += num;
                    count += 1;
                    break;
                case COUNT:
                    count += 1;
                    break;
                case MAX:
                    total = Math.max(total, num);
                    break;
                case MIN:
                    total = Math.min(total, num);
                    break;
                case SUM:
                    total += num;
                    break;
                case SC_AVG:
                    // TODO LATER
                    break;
                case SUM_COUNT:
                    // TODO LATER
                    break;
                default:
                    throw new IllegalArgumentException("Op " + op + " are not supported.");
            }
        }

        public Integer getResult() {
            switch (op) {
                case AVG: return total / count;
                case COUNT: return count;
                case MAX:
                case SUM:
                case MIN:
                    return total;
                case SC_AVG:
                    // TODO LATER
                    return total;
                case SUM_COUNT:
                    // TODO LATER
                    return total;
                default:
                    throw new IllegalArgumentException("Op " + op + " are not supported.");
            }
        }
    }



    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(needGroup && !gbfieldtype.equals(tup.getField(gbfield).getType())) {
            throw new IllegalArgumentException("Except groupType is: ["+ gbfieldtype + "] ,But given "+ tup.getField(gbfield).getType() + ".");
        }
        if(!(tup.getField(afield) instanceof IntField)) {
            throw new IllegalArgumentException("Except aggType is: [IntField] ,But given "+ tup.getField(afield) + ".");
        }
        IntField aggFeild = (IntField) tup.getField(afield);
        Field key = tup.getField(gbfield);
        if(groupMap.containsKey(key)) {
            groupMap.get(key).update(((IntField) tup.getField(afield)).getValue());
        } else {
            groupMap.put(key, new GroupResut(((IntField) tup.getField(afield)).getValue(), what));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            private Iterator<Map.Entry<Field, GroupResut>> iterator;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                iterator = IntegerAggregator.this.groupMap.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(iterator != null && iterator.hasNext()) return true;
                else return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Map.Entry<Field, GroupResut> next = iterator.next();
                Tuple tuple = new Tuple(IntegerAggregator.this.aggDesc);;
                if(IntegerAggregator.this.needGroup) {
                    tuple.setField(0, next.getKey());
                    tuple.setField(1, new IntField(next.getValue().getResult()));
                }
                else tuple.setField(0, new IntField(next.getValue().getResult()));
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return IntegerAggregator.this.aggDesc;
            }

            @Override
            public void close() {
                iterator = null;
            }
        };
    }

}
