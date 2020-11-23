package simpledb.jdbc.network;

import simpledb.file.FileMgr;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * 远程连接的RMI服务器端实现。
 * The RMI server-side implementation of RemoteConnection.
 *
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
    private SimpleDB db;
    private Transaction currentTx;
    private Planner planner;

    /**
     * 创建远程连接并为其启动新事务。
     * Creates a remote connection
     * and begins a new transaction for it.
     *
     * @throws RemoteException
     */
    RemoteConnectionImpl(SimpleDB db) throws RemoteException {
        this.db = db;
        currentTx = db.newTx();
        planner = db.planner();
    }

    /**
     * Creates a new RemoteStatement for this connection.
     *
     * @see RemoteConnection#createStatement()
     */
    @Override
    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this, planner);
    }

    /**
     * Closes the connection.
     * The current transaction is committed.
     *
     * @see RemoteConnection#close()
     */
    @Override
    public void close() throws RemoteException {
        currentTx.commit();
    }

// The following methods are used by the server-side classes.

    /**
     * Returns the transaction currently associated with
     * this connection.
     *
     * @return the transaction associated with this connection
     */
    Transaction getTransaction() {
        return currentTx;
    }

    /**
     * 提交当前事务，并开始一个新事务。
     * Commits the current transaction,
     * and begins a new one.
     */
    void commit() {
        currentTx.commit();
        currentTx = db.newTx();
        FileMgr fm = db.fileMgr();
        System.out.println("写了" + fm.countWriteBlockNums() + "页内存缓冲区的文件到磁盘上的文件的块");
        System.out.println("读了" + fm.countReadBlockNums() + "块磁盘上的文件的块到内存缓冲区");
//        fm.countReadBlockNums();
//        fm.countWriteBlockNums();
    }

    /**
     * 回滚当前事务，并开始新事务。
     * Rolls back the current transaction,
     * and begins a new one.
     */
    void rollback() {
        currentTx.rollback();
        currentTx = db.newTx();
        FileMgr fm = db.fileMgr();
        System.out.println("写了" + fm.countWriteBlockNums() + "页内存缓冲区的文件到磁盘上的文件的块");
        System.out.println("读了" + fm.countReadBlockNums() + "块磁盘上的文件的块到内存缓冲区");
    }
}

