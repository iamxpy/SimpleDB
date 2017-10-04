package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    //Key相当于资源，LockState存放事务id与锁类型，故每个LockState代表某事务在Key上加了锁
    //故整个map为所有资源的锁信息
    private Map<PageId, List<LockState>> lockStateMap;

    //Key为事务，PageId为正在等待的资源，相当于保存了等待的信息，PS：BufferPool中实际用的是sleep体现等待
    private Map<TransactionId, PageId> waitingInfo;

    public LockManager() {
        //使用支持并发的容器避免ConcurrentModificationException
        lockStateMap = new ConcurrentHashMap<>();
        waitingInfo = new ConcurrentHashMap<>();
    }


//==========================申请锁,加锁,解锁的相关方法 begin==================================

    /**
     * 如果tid已经在pid上有读锁，返回true
     * 如果tid在pid上已经有写锁，或者没有锁但条件允许tid给pid加读锁，则加锁后返回true
     * 如果tid此时不能给pid加读锁，返回false
     *
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean grantSLock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {//pid上只有一个锁
                LockState ls = list.iterator().next();
                if (ls.getTid().equals(tid)) {//判断是否为自己的锁
                    //如果是读锁，直接返回（在||的之前返回），否则加锁再返回
                    return ls.getPerm() == Permissions.READ_ONLY || lock(pid, tid, Permissions.READ_ONLY);
                } else {
                    //如果是别人的读锁，加锁再返回，是写锁则需要等待
                    return ls.getPerm() == Permissions.READ_ONLY ? lock(pid, tid, Permissions.READ_ONLY) : wait(tid, pid);
                }
            } else {
                //多个锁有四种情况
                // 1.两个锁，且都属于tid（一读一写）    2.两个锁，且都属于非tid的事务（一读一写）
                // 3.多个读锁，且其中有一个为tid的读锁  4.多个读锁，且没有tid的读锁
                for (LockState ls : list) {
                    if (ls.getPerm() == Permissions.READ_WRITE) {
                        //如果其中有一个写锁，那么根据是否为自己的来判断属于情况1还是2
                        return ls.getTid().equals(tid) || wait(tid, pid);
                    } else if (ls.getTid().equals(tid)) {//如果是读锁且是tid的
                        return true;//情况3在此返回，也可能是情况1（如果先遍历到读锁）
                    }
                }
                //情况4
                return lock(pid, tid, Permissions.READ_ONLY);
            }
        } else {
            return lock(pid, tid, Permissions.READ_ONLY);
        }
    }

    /**
     * 如果tid已经在pid上有写锁，则返回true
     * 如果仅tid拥有pid的读锁，或tid在pid上没有锁但条件允许tid给pid加写锁，则加锁后返回true
     * 如果tid此时不能给pid加写锁，返回false
     *
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean grantXLock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {//如果pid上只有一个锁
                LockState ls = list.iterator().next();
                //如果是自己的写锁，直接返回（在||之前返回），否则加锁再返回（在lock处返回）
                //如果这个锁是别人的，必须等待，也就是在wait处（冒号之后）返回
                return ls.getTid().equals(tid) ? ls.getPerm() == Permissions.READ_WRITE || lock(pid, tid, Permissions.READ_WRITE) : wait(tid, pid);
            } else {
                //多个锁有三种情况，只有第一种情况返回true，其余返回wait
                // 1.两个锁，且都属于tid（一读一写） 2.两个锁，且都属于非tid的事务（一读一写） 3.多个读锁
                if (list.size() == 2) {
                    for (LockState ls : list) {
                        if (ls.getTid().equals(tid) && ls.getPerm() == Permissions.READ_WRITE) {
                            return true;//两个锁而且有一个自己的写锁
                        }
                    }
                }
                return wait(tid, pid);
            }
        } else {//pid上没有锁，可以加写锁
            return lock(pid, tid, Permissions.READ_WRITE);
        }
    }


    /**
     * 加锁，表示tid在pid上有一个perm权限的锁，并返回true
     * @param pid
     * @param tid
     * @param perm
     */
    private synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm) {
        LockState nls = new LockState(tid, perm);
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(nls);
        lockStateMap.put(pid, list);
        waitingInfo.remove(tid);
        return true;
    }

    /**
     * 只是处理好waitingInfo的信息然后返回false
     * @param tid
     * @param pid
     * @return
     */
    private synchronized boolean wait(TransactionId tid, PageId pid) {
        waitingInfo.put(tid, pid);
        return false;
    }


    /**
     * unlock被设计为可以随时调用，如果不存在则返回false
     * 这样，查找是否存在的代码已经在方法内，在其他地方不必先确认存在再unlock
     * 而是应该先unlock再根据返回结果判断是否存在
     *
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean unlock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStateMap.get(pid);

        if (list == null || list.size() == 0) return false;
        LockState ls = getLockState(tid, pid);
        if (ls == null) return false;
        list.remove(ls);
        lockStateMap.put(pid, list);
        return true;
    }

    /**
     * 释放事务tid拥有的所有锁
     *
     * @param tid
     */
    public synchronized void releaseTransactionLocks(TransactionId tid) {
        //先找出所有，再释放
        List<PageId> toRelease = getAllLocksByTid(tid);
        for (PageId pid : toRelease) {
            unlock(tid, pid);
        }
    }

//==========================申请锁,加锁,解锁的相关方法 end==================================


//==========================检测死锁的相关方法 beign======================================

    /**
     *
     * 通过检测资源的依赖图根据是否存在环来判断是否已经陷入死锁
     * 具体实现：本事务tid需要检测“正在等待的资源的拥有者是否已经直接或间接的在等待本事务tid已经拥有的资源”
     * <p>
     * 如图，括号内P1,P2,P3为资源,T1,T2,T3为事务
     * 虚线以及其上的字母R加上箭头组成了拥有关系，如果是字母W则代表正在等待写锁
     * 例如下图左上方T1到P1的一连串符号表示的是T1此时拥有P1的读锁
     * 图的边缘可以是虚线的转折点，例如为了表示T2正在等待P1
     * <p>
     * //     T1---R-->P1<-------
     * //                       W
     * //  ----------------------
     * //  W
     * //  ---T2---R-->P2<-------
     * //                       W
     * //  ----------------------
     * //  W
     * //  ---T3---R-->P3
     * <p>
     * 上图的含义是，Ti拥有了对Pi的读锁(1<=i<=3)
     * 因为T1在P1上有了读锁，所以T2正在等待P1的写锁
     * 同理，T3正在等待P2的写锁
     * <p>
     * 现在假设的情景是，此时T1要申请对P3的写锁，进入等待，这将会造成死锁
     * 而接下来调用这个方法判断，就可以得知已经产生死锁从而回滚事务（具体在BufferPool的getPage()方法的while循环开始处）
     * <p>
     * 导致死锁的本质原因就是将等待的资源(P3)的拥有者(T3)间接的在等待T1拥有的资源(P1)
     * 下面方法的注释以这个例子为基础，具体解释这个方法是如何判断出“T1在P3上的等待已经造成了死锁”的
     *
     * @param tid
     * @param pid
     * @return true表示进入了死锁，false表示没有
     */
    public synchronized boolean deadlockOccurred(TransactionId tid, PageId pid) {//T1为tid，P3为pid
        List<LockState> holders = lockStateMap.get(pid);
        if (holders == null || holders.size() == 0) {
            return false;
        }
        List<PageId> pids = getAllLocksByTid(tid);//找出T1拥有的所有资源，即只含有P1的list
        for (LockState ls : holders) {
            TransactionId holder = ls.getTid();
            //去掉T1，因为虽然上图没画出这种情况，但T1可能同时也在其他Page上有读锁，这会影响判断结果
            if (!holder.equals(tid)) {
                //判断T3(holder)是否直接或间接在等待P1(pids)
                //由图可以看出T3在直接等待P2，而P2的拥有者T2在直接等待P1,即T3在间接等待P1
                boolean isWaiting = isWaitingResources(holder, pids, tid);
                if (isWaiting) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断tid是否直接或间接地在等待pids中的某个资源
     *
     * @param tid
     * @param pids
     * @param toRemove 需要排除toRemove来判断，具体原因见方法内部注释；
     *                 事实上，toRemove就是leadToDeadLock()的参数tid，也就是要排除它自己对判断过程的影响
     * @return
     */
    private synchronized boolean isWaitingResources(TransactionId tid, List<PageId> pids, TransactionId toRemove) {
        PageId waitingPage = waitingInfo.get(tid);
        if (waitingPage == null) {
            return false;
        }
        for (PageId pid : pids) {
            if (pid.equals(waitingPage)) {
                return true;
            }
        }
        //到达这里说明tid并不直接在等待pids中的任意一个，但有可能间接在等待
        //如果waitingPage的拥有者们(去掉toRemove)中的某一个正在等待pids中的某一个，说明是tid间接在等待
        List<LockState> holders = lockStateMap.get(waitingPage);
        if (holders == null || holders.size() == 0) return false;//该资源没有拥有者
        for (LockState ls : holders) {
            TransactionId holder = ls.getTid();
            if (!holder.equals(toRemove)) {//去掉toRemove，在toRemove刚好拥有waitingResource的读锁时就需要
                boolean isWaiting = isWaitingResources(holder, pids, toRemove);
                if (isWaiting) return true;
            }
        }
        //如果在for循环中没有return，说明每一个holder都不直接或间接等待pids
        //故tid也非间接等待pids
        return false;
    }

//==========================检测死锁的相关方法 end======================================


//==========================查询与修改两个map信息的相关方法 beign=========================

    /**
     * @param tid 施加锁的事务id
     * @param pid 被上锁的page
     * @return tid代表的事务在pid上的锁;如果不存在该锁，返回null
     */
    public synchronized LockState getLockState(TransactionId tid, PageId pid) {
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

    /**
     * 得到tid所拥有的所有锁，以锁所在的资源pid的形式返回
     *
     * @param tid
     * @return
     */
    private synchronized List<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> pids = new ArrayList<>();
        for (Map.Entry<PageId, List<LockState>> entry : lockStateMap.entrySet()) {
            for (LockState ls : entry.getValue()) {
                if (ls.getTid().equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
        return pids;
    }

//==========================查询与修改两个map信息的相关方法 end=========================

}
