package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;

    private TupleDesc tupleDesc;

    private OpIterator[] childs;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.tupleDesc = child.getTupleDesc();
        this.childs = new OpIterator[1];
        this.childs[0] = child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        childs[0].open();
    }

    public void close() {
        super.close();
        childs[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        childs[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(childs[0].hasNext()) {
            Tuple tuple = childs[0].next();
            if(predicate.filter(tuple)) return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return childs;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.childs = children;
    }

}
