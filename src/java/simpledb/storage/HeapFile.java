package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
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
    final private File f;
    final private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        return f.getAbsoluteFile().hashCode();
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
        int tableId = pid.getTableId();
        int pgno = pid.getPageNumber();
        RandomAccessFile raf = null;
        try{
             raf = new RandomAccessFile(f,"r");
             int begin = pgno * BufferPool.getPageSize();
             int end = (pgno+1) * BufferPool.getPageSize();
             if(end>raf.length()){
                 raf.close();
                 throw new IllegalArgumentException(String.format("Table %d Page %d is not found in File %s",tableId, pgno,f.getAbsolutePath()));
             }
             raf.seek(begin);
             byte[] data = new byte[BufferPool.getPageSize()];
             try{
                 raf.readFully(data);
             }catch(EOFException e){
                 raf.close();
                 throw new IllegalArgumentException(String.format("Table %d Page %d is out of range in File %s",tableId, pgno,f.getAbsolutePath()));
             }catch(IOException e){
                 raf.close();
                 throw new IllegalArgumentException(String.format("Table %d Page %d is not found in File %s",tableId, pgno,f.getAbsolutePath()));
             }
             HeapPageId newPid = new HeapPageId(tableId, pgno);
             return new HeapPage(newPid,data);

        }catch(IOException e){
            e.printStackTrace();
        }finally {
            try{
                raf.close();
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();

        return (int)Math.floor((double)f.length()/pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    public class HeapFileIterator implements DbFileIterator{
        private final HeapFile f;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private int nowPage;
        public HeapFileIterator(HeapFile f,TransactionId tid) {
            this.f = f;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            nowPage = 0;
            it = tupleIterator(nowPage);
        }

        private Iterator<Tuple> tupleIterator(int pageIndex) throws DbException, TransactionAbortedException {
            if(pageIndex>=0||pageIndex<f.numPages()){
                HeapPageId pid = new HeapPageId(f.getId(), pageIndex);
                HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                return p.iterator();
            }else{
                throw new DbException(String.format("File %s have not page %d",f.getFile().getAbsolutePath(),pageIndex));
            }

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it == null){
                return false;
            }
            if(it.hasNext()){
                return true;
            }else{
                if(nowPage < f.numPages()-1){
                    nowPage++;
                    it = tupleIterator(nowPage);
                    return it.hasNext();
                }else{
                    return false;
                }
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException {
            if(it.hasNext()){
                return it.next();
            }else{
                throw new NoSuchElementException();
            }
        }

        @Override
        public void close(){
            it = null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

}

