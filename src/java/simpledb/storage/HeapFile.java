package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        tableId = f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // 直接从disk中读取，而不通过缓存
        int pageSize = BufferPool.getPageSize();
        byte[] pageBytes = new byte[pageSize];
        int startPos = pid.getPageNumber() * pageSize; //start index of this page
        try {
            RandomAccessFile f = new RandomAccessFile(file, "r");
            f.seek(startPos);
            f.read(pageBytes, 0, pageSize);//这里有个坑,off参数是pageBytes的,也就是从pageBytes的off位开始写,而不是file中的,所以应该先seek
            return new HeapPage((HeapPageId) pid, pageBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int startPos = BufferPool.getPageSize() * page.getId().getPageNumber();
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek(startPos);
        f.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 这里我纠结了好久,一个file里边就是一个一个page挨着放的嘛,这么简单的道理我半天没想明白🤣
        return (int) file.length() / BufferPool.getPageSize();
    }

    //todo 对于insertTuple、deleteTuple是否要调用writePage不是很确定。目前暂时需要调用，理由是HeapPage是在内存中，即使对page进行修改（insert/delete）
    // 但都只是对内存的修改，还没有完成持久化。但我觉得目前的考虑也许是多余的，等到后边的lab实现事务的时候一定会考虑这方面的问题，如果是STEAL mode，则需要
    // 同时进行apply；如果是NO-FORCE，则不需要就在这里进行apply

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        BufferPool bufferPool =Database.getBufferPool();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage)bufferPool.getPage(tid, new HeapPageId(tableId, i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                return Collections.singletonList(page);
            }
        }
        // 不能在现有page中找到slot，需要新创建一个page并进行insert
        HeapPage newPage = new HeapPage(new HeapPageId(tableId, numPages()), HeapPage.createEmptyPageData());
        //todo writePage在这里调用应该就不会出现在事务提交前就写入的情况，因为这里写入的其实是一个emptyPageData
        // 下边的insertTuple其实只改变了这个page在内存中的状态，并没进行刷盘。
        // 其实下边这些操作都是因为如TransactionTest的要求，里边的setUp方法需要初始化三个new page
        writePage(newPage); //apply
        newPage.insertTuple(t);
        return Collections.singletonList(newPage);
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // some code goes here
        if (tableId != t.getRecordId().getPageId().getTableId()) {
            throw new DbException("the tuple is not the member of this file(table)");
        }
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);

        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new Iter(tid);
    }

    /**
     * iter each tuple of a table
     */
    class Iter implements DbFileIterator{
        /*
        总的目标是遍历整个table中的所有tuple，但这里要求“惰性”，也就是不把整个表读取到iter中，而是一页一页读取：
        当一个page遍历完毕后，再读取下一个page，详细看tupleIter()方法。
         */
        private int curPageNo;
        private Iterator<Tuple> curTupleIter;
        private boolean isOpen=false;
        private final TransactionId tid;

        Iter(TransactionId tid){
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // todo 是否需要保证open和close的幂等
            if (isOpen) return;
            isOpen = true;
            curPageNo = 0;//从0开始计数,所以有效的下标应该是[0,numPages()-1]
            curTupleIter = tupleIter();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen)
                return false;
            while (true) {
                if (curTupleIter.hasNext())
                    return true;
                curPageNo++;
                if (curPageNo > numPages() -1)
                    return false;
                curTupleIter = tupleIter();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen)
                throw new NoSuchElementException("iter haven't been open");
            // lab 2.3中发现的一个问题，当前page已经遍历完毕了，但是下一个page可能也是空的，所以获得了下一个page的iter后不能贸然返回，而是需要
            // 再次判断一下当前page是否有tuple
            while (true) {
                if (curTupleIter.hasNext())
                    return curTupleIter.next();
                curPageNo++;
                if (curPageNo > numPages()-1)
                    throw new NoSuchElementException();
                curTupleIter = tupleIter();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            if (!isOpen) return;
            isOpen = false;
            curPageNo = 0;
        }

        /**
         * 加载下一个页面(curPageNo+1)的迭代器。通过buffer pool来加载，而不是直接从磁盘中
         */
        Iterator<Tuple> tupleIter() throws TransactionAbortedException, DbException {
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), curPageNo), Permissions.READ_ONLY);
            return page.iterator();
        }

    }

}

