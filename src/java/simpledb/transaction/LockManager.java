package simpledb.transaction;

import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public ConcurrentHashMap<PageId, PageContext> pageContexts;
    public long DEFAULT_TIMEOUT = 1000;

    public LockManager(){
        this.pageContexts = new ConcurrentHashMap<>();
    }

    public boolean lock(TransactionId trxId, PageId pageId, Permissions permissions) throws TransactionAbortedException {
        return lock(trxId, pageId, permissions, DEFAULT_TIMEOUT + new Random().nextInt(500)); //1000~1500ms
    }

    // 似乎还没法用rwlock来实现，因为这里用不了线程，因为一个事务可能会用两次以上的不同线程来操作，
    // 也不支持锁升级（特指读锁升级为写锁），同时可重入也带来了很多麻烦。
    // 详见：https://www.jianshu.com/p/9cd5212c8841
    public boolean lock(TransactionId trxId, PageId pageId,
                                     Permissions permissions,
                                     long timeout) throws TransactionAbortedException {
        long startTime = System.currentTimeMillis();
        while (!tryAcquire(trxId, pageId, permissions)) {
            if (System.currentTimeMillis() > timeout + startTime)//实现死锁检测，超时便认为产生了死锁
                throw new TransactionAbortedException(); //abort
        }
        return true;
    }

    boolean tryAcquire(TransactionId trxId, PageId pageId, Permissions permissions) {
        if (!pageContexts.containsKey(pageId)) {
            pageContexts.put(pageId, new PageContext(pageId));
        }

        // 目前想到的比较好的方式是采用下边这种，讲锁的粒度降低到pageContext，而不是用LockManager对象锁
        PageContext context = pageContexts.get(pageId);
        // 这里有一个提示：Synchronization on local variable 'context'。但应该是没有问题的，因为我这里虽然是给一个本地变量加锁，
        // 但是最终还是获取的是这个本地变量所指向的堆中对象的monitor
        synchronized (context) {
            if (permissions == Permissions.READ_ONLY) { //share
                // todo 这里是否需要支持降级？比如之前是排他锁，然后变成共享锁
                if (context.readTrxs.contains(trxId)) return true; //重入
                if (context.writeTrx != null) return false; //当前是排他锁
                context.readTrxs.add(trxId);
                return true;
            }
            if (permissions == Permissions.READ_WRITE) { //exclusive
                if (context.writeTrx != null && context.writeTrx == trxId)
                    return true; //重入
                if (context.writeTrx == null && context.readTrxs.size() == 0) {
                    context.writeTrx = trxId;
                    context.readTrxs.add(trxId);//和下一种情况的区别在这里
                    return true;
                }
                if (context.writeTrx == null && context.readTrxs.size() == 1 && context.readTrxs.contains(trxId)) {
                    //当前只有一个共享锁，且是自己
                    context.writeTrx = trxId;
                    return true;
                }
                return false;
            }
        }
        try {
            throw new DbException("unexpected permission type:"+permissions);
        } catch (DbException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean unlock(TransactionId trxId, PageId pageId) {
        if (!pageContexts.containsKey(pageId)) {
            return true;
        }

        PageContext context = pageContexts.get(pageId);
        synchronized (context) {
            context.readTrxs.remove(trxId);
            if (context.writeTrx != null && context.writeTrx.equals(trxId))
                context.writeTrx = null;
            return true;
        }

    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        if (!pageContexts.containsKey(p)) return false;
        return pageContexts.get(p).readTrxs.contains(tid);
    }

    static class PageContext {
        PageId pageId;
        Set<TransactionId> readTrxs; //shared
        TransactionId writeTrx; // exclusive

        public PageContext(PageId pageId) {
            this.pageId = pageId;
            readTrxs = new HashSet<>();
            writeTrx = null;
        }
    }
}
