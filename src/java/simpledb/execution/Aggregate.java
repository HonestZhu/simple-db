package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator[] children;

    private int aggFieldIndex;
    private int groupByIndex;
    private Aggregator.Op aggOp;
    private TupleDesc tupleDesc;
    private TupleDesc aggDesc;
    private Aggregator aggregator;

    private boolean needGroup;
    /**
     * 存放aggregator处理后的结果
     */
    private OpIterator opIterator;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.children = new OpIterator[]{child};
        this.aggFieldIndex = afield;
        this.groupByIndex = gfield;
        this.aggOp = aop;
        this.needGroup = gfield >= 0;
        this.tupleDesc = child.getTupleDesc();

        if(this.needGroup) {
            this.aggDesc = new TupleDesc(new Type[]{this.tupleDesc.getFieldType(gfield), Type.INT_TYPE}, new String[]{"groupVal", "aggVal"});
        } else {
            this.aggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggVal"});
//            this.aggregator = new IntegerAggregator(-1, null, afield, this.aggOp);
        }

        if(this.tupleDesc.getFieldType(afield) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, needGroup ? this.tupleDesc.getFieldType(gfield) : null, afield, this.aggOp);
        } else {
            this.aggregator = new StringAggregator(gfield, needGroup ? this.tupleDesc.getFieldType(gfield) : null, afield, this.aggOp);
        }


    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.groupByIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return this.tupleDesc.getFieldName(groupByIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aggFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return this.tupleDesc.getFieldName(aggFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.children[0].open();
        while (children[0].hasNext()) {
            Tuple nextTuple = children[0].next();
            aggregator.mergeTupleIntoGroup(nextTuple);
        }
        this.opIterator = aggregator.iterator();
        this.opIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.opIterator.hasNext()) {
            return this.opIterator.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.children[0].rewind();
        this.opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.aggDesc;
    }

    public void close() {
        // some code goes here
        this.children[0].close();
        this.opIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}