package simpledb;

public enum GrantResult{
    HAS_HELD,//表示申请该锁的事务已经获得该锁，不必再申请
    CAN_HOLD,//可以加锁
    MUST_WAIT//需要等待，阻塞直到被notify
}
