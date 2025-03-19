package Backpack;


import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class BasicBuffer implements Buffer {
    private int capacity;
    private int size;
    private ConcurrentHashMap<PageId, Page> pages;
    public BasicBuffer(int capacity) {
        this.capacity = capacity;
        this.pages = new ConcurrentHashMap<>(capacity);
    }
    @Override
    public Page getPage(PageId pid) throws TransactionAbortedException, DbException{
        if(pages.containsKey(pid)){
            return pages.get(pid);
        }
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p = f.readPage(pid);
        pages.put(pid, p);
        return p;
    }

    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> l = f.deleteTuple(tid, t);
        for(Page p:l){
            pages.put(p.getId(),p);
        }
    }

    @Override
    public void insertTuple(TransactionId tid,int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException{
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> l = f.insertTuple(tid, t);
        for(Page p:l){
            pages.put(p.getId(),p);
        }
    }

    @Override
    public synchronized void flushAllPages() throws IOException {

    }
}
