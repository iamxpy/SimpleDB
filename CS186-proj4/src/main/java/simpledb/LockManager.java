package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LockManager {
    private Map<PageId, List<LockState>> lockStateMap;

    public LockManager() {
        lockStateMap = new HashMap<>();
    }

    /**
     * @param pid
     * @return 该page是否被锁
     */
    public boolean isPageLocked(PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        return list != null && list.size() != 0;
    }

    /**
     * @param pid
     * @return 该page是否可以加读锁
     */
    public boolean pageReadable(PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        if (list != null && list.size() == 1) {
            for (LockState ls : list) {
                return ls.getPerm().equals(Permissions.READ_ONLY);
            }
        }
        //如果是null，或者不为null但长度为0（说明没有锁）或大于1（说明有多个读锁），都可以加读锁
        return true;
    }

    public void lock(PageId pid, LockState lockState) {
        synchronized (pid) {
            ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(lockState);
            lockStateMap.put(pid, list);
        }
    }

    /**
     * @param tid 施加锁的事务id
     * @param pid 被上锁的page
     * @return tid代表的事务在pid上的锁;如果不存在该锁，返回null
     */
    private LockState getLockState(TransactionId tid, PageId pid) {
        synchronized (pid) {
            ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
            if (list == null || list.size() == 0) {
                return null;
            }
            for (LockState ls : list) {
                if (ls.getTid().equals(tid)) {//找到了对应的锁
                    return ls;
                }
            }
            return null;
        }
    }

    public void unlock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
            if (list == null || list.size() == 0) {
                throw new IllegalArgumentException("page "+pid+" does not locked by any transaction");
            }
            LockState ls = getLockState(tid, pid);
            if (ls == null) {
                throw new IllegalArgumentException("Transaction " + tid + " dose not lock the page " + pid);
            }
            list.remove(ls);
            lockStateMap.put(pid, list);
            //如果当前线程已经没有锁，通知别的线程来争夺该page的锁
            if (!isPageLocked(pid)) {
                pid.notifyAll();
            }
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
            if (list != null) {
                for (LockState ls : list) {
                    if (ls.getTid().equals(tid)) return true;
                }
            }
            return false;
        }
    }

    /**
     * 如果pid对应page上只有一把锁而且该锁为tid拥有的读锁，则该锁是可以被升级为写锁的
     *
     * @param pid
     * @param tid
     * @return 返回tid对应的事务是否有权将pid对应的page上的锁升级为读锁
     */
    public boolean lockUpgradable(PageId pid, TransactionId tid) {
        synchronized (pid) {
            ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
            if (list != null && list.size() == 1) {
                LockState ls = getLockState(tid, pid);
                if (ls != null) {
                    return ls.getPerm().equals(Permissions.READ_ONLY);
                }
            }
            return false;
        }
    }
}