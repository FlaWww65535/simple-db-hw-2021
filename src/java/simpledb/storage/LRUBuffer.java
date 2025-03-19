package simpledb.storage;


import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LRUBuffer implements Buffer {
    private int capacity;
    public class Node<V>{
        private V value;
        Node<V> prev;
        Node<V> next;
        Node(V value){
            this.value = value;
        }
    }

    private ConcurrentHashMap<PageId, Node<Page>> pages;
    private Node<Page> head;
    private Node<Page> tail;

    public LRUBuffer(int capacity) {
        this.capacity = capacity;
        this.pages = new ConcurrentHashMap<>(capacity);
        head = new Node<>(null);
        tail = new Node<>(null);
        head.next = tail;
        tail.prev = head;
    }

    private void moveToHead(Node<Page> node){
        deleteNode(node);
        addToHead(node);
    }

    private void addToHead(Node<Page> node){
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
        node.prev = head;
    }

    private void deleteNode(Node<Page> node){
        node.prev.next=node.next;
        node.next.prev=node.prev;
    }

    private void addPageToBuffer(Page p)throws DbException{
        //If buffer is full,evict one Page then add.
        if(pages.size() >= capacity){
            evictPage();
        }
        PageId pid = p.getId();
        Node<Page> n = new Node<>(p);
        addToHead(n);
        pages.put(pid, n);
    }

    private void updatePageInBuffer(PageId pid) {
        Node<Page> n = pages.get(pid);
        moveToHead(n);
    }


    @Override
    public Page getPage(PageId pid) throws TransactionAbortedException, DbException{
        if(pages.containsKey(pid)){
            updatePageInBuffer(pid);
            return pages.get(pid).value;
        }
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p = f.readPage(pid);
        addPageToBuffer(p);
        return p;
    }

    @Override
    public void deleteTuple(TransactionId tid,Tuple t) throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> l = f.deleteTuple(tid, t);
        for(Page p:l){
            addPageToBuffer(p);
        }
    }

    @Override
    public void insertTuple(TransactionId tid,int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException{
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> l = f.insertTuple(tid, t);
        for(Page p:l){
            addPageToBuffer(p);
        }
    }

    @Override
    public synchronized void flushAllPages() throws IOException {
        for(PageId pid:pages.keySet()){
            flushPage(pid);
        }
    }

    @Override
    public synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = pages.get(pid).value;
        if(p.isDirty()!=null){
            DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
            f.writePage(p);
        }
    }

    @Override
    public synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page p = tail.prev.value;
        if(p.isDirty()!=null){
            try{
                flushPage(p.getId());
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        discardPage(p.getId());
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        deleteNode(pages.get(pid));
        pages.remove(pid);
    }
}
