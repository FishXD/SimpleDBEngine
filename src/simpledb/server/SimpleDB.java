package simpledb.server;

import java.io.File;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.buffer.BufferMgr;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.*;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.opt.HeuristicQueryPlanner;

/**
 * The class that configures the system.
 * 
 * @author Edward Sciore
 */
public class SimpleDB {
   public static int BLOCK_SIZE = 400;
   public static int BUFFER_SIZE = 8;
   public static String LOG_FILE = "simpledb.log";

   private  FileMgr     fm;
   private  BufferMgr   bm;
   private  LogMgr      lm;
   private  MetadataMgr mdm;
   private  Planner planner;

   /**
    * A constructor useful for debugging.
    * @param dirname 数据库目录的名称
    * @param blockSize 磁盘上的块的大小
    * @param buffSize 缓冲区管理器的插槽个数(一个插槽可以容纳一个缓冲区)
    */
   public SimpleDB(String dirname, int blockSize, int buffSize) {
      File dbDirectory = new File(dirname);
      fm = new FileMgr(dbDirectory, blockSize);
      lm = new LogMgr(fm, LOG_FILE);
      bm = new BufferMgr(fm, lm, buffSize);
   }
   
   /**
    * 大多数情况下的一个更简单的构造函数。与需要3个参数的构造函数不同，它还初始化元数据表。
    * A simpler constructor for most situations. Unlike the
    * 3-arg constructor, it also initializes the metadata tables.
    *
    * @param dirname the name of the database directory
    */
   public SimpleDB(String dirname) {
      this(dirname, BLOCK_SIZE, BUFFER_SIZE); 
      Transaction tx = newTx();
      boolean isnew = fm.isNew();
      if (isnew) {
         System.out.println("creating new database");
      } else {
         System.out.println("recovering existing database");
         tx.recover();
      }
      mdm = new MetadataMgr(isnew, tx);
      QueryPlanner qp = new BasicQueryPlanner(mdm);
      UpdatePlanner up = new BasicUpdatePlanner(mdm);
//    QueryPlanner qp = new HeuristicQueryPlanner(mdm);
//    UpdatePlanner up = new IndexUpdatePlanner(mdm);
      planner = new Planner(qp, up);
      tx.commit();
   }
   
   /**
    * A convenient way for clients to create transactions
    * and access the metadata.
    */
   public Transaction newTx() {
      return new Transaction(fm, lm, bm);
   }
   
   public MetadataMgr mdMgr() {
      return mdm;
   }
   
   public Planner planner() {
      return planner;
   }


   // These methods aid in debugging
   /**
    *
    * @return 这里返回的是fm, 而fm是前面调用SimpleDB构造方法的时候已经进行了初始化操作，已经成为了FileMgr对象
    */
   public FileMgr fileMgr() {
      return fm;
   }

   /**
    *
    * @return 日志管理对象
    */
   public LogMgr logMgr() {
      return lm;
   }   
   public BufferMgr bufferMgr() {
      return bm;
   }   
 }
