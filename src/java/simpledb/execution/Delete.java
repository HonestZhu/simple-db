package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator[] childs;
    private TupleDesc tupleDesc;
    private Tuple res;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.childs = new OpIterator[1];
        this.childs[0] = child;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"deleteNums"});
        this.res = null;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.childs[0].open();
        this.res = null;
    }

    public void close() {
        super.close();
        this.childs[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(res != null) return null;
        int cnt = 0;
        while(childs[0].hasNext()) {
            Tuple tuple = childs[0].next();
            try {
                Database.getBufferPool().deleteTuple(t, tuple);
                cnt ++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        res = new Tuple(tupleDesc);
        res.setField(0, new IntField(cnt));
        return res;
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
