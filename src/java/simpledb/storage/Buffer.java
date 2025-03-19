package simpledb.storage;

import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

public interface Buffer {
    Page getPage(PageId pageId)
            throws TransactionAbortedException, DbException;

    void insertTuple(TransactionId tid,int tableId,Tuple t)
            throws DbException, IOException, TransactionAbortedException;

    void deleteTuple(TransactionId tid,Tuple t)
            throws DbException, IOException, TransactionAbortedException;
    void flushAllPages() throws IOException;
    void flushPage(PageId pid) throws IOException;
    void evictPage() throws DbException;
    void discardPage(PageId pid);

}
