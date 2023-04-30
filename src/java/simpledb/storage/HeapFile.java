package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * f the file that stores the on-disk backing store for this heap file.
     */
    private final File f;

    /**
     * 文件描述（consist of records）
     */
    private final TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        // 获取目标页的offset
        int offset = pgNo * BufferPool.getPageSize();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(f, "r")) {
            if((pgNo + 1) * BufferPool.getPageSize() > randomAccessFile.length())
                throw new IllegalArgumentException(String.format("[readPage]: table %d page %d is invalid.", tableId, pgNo));
            byte[] bytes = new byte[BufferPool.getPageSize()];
            // 移动起始位置
            randomAccessFile.seek(offset);
            int read = randomAccessFile.read(bytes, 0, BufferPool.getPageSize());
            if(read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("[readPage]: table %d page %d read %d bytes not equal to BufferPool.getPageSize().", tableId, pgNo, read));
            }
            HeapPageId heapPageId = new HeapPageId(tableId, pgNo);
            return new HeapPage(heapPageId, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException(String.format("[readPage]: table %d page %d is invalid.", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getId();
        int pageNo = pageId.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile file = new RandomAccessFile(this.f, "rw");
        file.seek(offset);
        file.write(pageData);
        file.close();

        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor((double) this.getFile().length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if(page.getNumUnusedSlots() == 0) continue;
            page.insertTuple(t);
            pages.add(page);
            return pages;
        }
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f,true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();
        // 加载进BufferPool
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                new HeapPageId(getId(),numPages()-1),Permissions.READ_WRITE);
        p.insertTuple(t);
        pages.add(p);
        return pages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        List<Page> pages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), t.getRecordId().getPageId().getPageNumber()), Permissions.READ_WRITE);
        page.deleteTuple(t);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new AbstractDbFileIterator() {
            private Iterator<Tuple> iterator = null;
            private HeapPage page = null;
            private int pageNo = 0;

            /**
             * Opens the iterator
             *
             * @throws DbException when there are problems opening/accessing the database.
             */
            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.pageNo = 0;
                PageId pageId = new HeapPageId(HeapFile.this.getId(), this.pageNo);
                this.page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                this.iterator = this.page.iterator();
            }

            /**
             * Resets the iterator to the start.
             *
             * @throws DbException When rewind is unsupported.
             */
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }

            /**
             * Reads the next tuple from the underlying source.
             *
             * @return the next Tuple in the iterator, null if the iteration is finished.
             */
            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (this.iterator == null) {
                    return null;
                }
                if (this.iterator.hasNext()) {
                    return this.iterator.next();
                }
                // 如果读完当前页，那么去寻找下一个非空页
                while (this.pageNo + 1 < HeapFile.this.numPages()) {
                    PageId pageId = new HeapPageId(HeapFile.this.getId(), ++this.pageNo);
                    this.page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                    this.iterator = this.page.iterator();
                    if (this.iterator.hasNext()) {
                        // the new page has Tuple in the iterator
                        return this.iterator.next();
                    }
                }
                // all page iterator finish
                this.iterator = null;
                return null;
            }

            /**
             * Closes the iterator.
             */
            @Override
            public void close() {
                super.close();
                this.iterator = null;
                this.page = null;
            }

        };
    }

}

