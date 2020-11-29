package simpledb.log;

import simpledb.file.Page;
import simpledb.server.SimpleDB;

import java.util.Iterator;

/**
 * 代码创建了70条日志记录，每个日志记录由一个字符串和一个整数组成。
 * 整数是记录号N，字符串是值“recordN”。
 * 代码在创建前35条记录后打印一次，然后在全部创建70条记录后再打印一次。
 *
 * @author Edward Sciore
 */
public class LogTest {
    private static LogMgr lm;

    public static void main(String[] args) {
        // 分别在磁盘上和缓冲区创建日志文件和日志页
        SimpleDB db = new SimpleDB("logtest", 400, 8);
        lm = db.logMgr();

        // 从磁盘的日志文件中读取
        printLogRecords("The initial empty log file:");
        System.out.println("done");
        createRecords(1, 35);
        // 第一次调用printLogRecords之后只打印了20条记录。
        // 原因是这些记录填满了第一个日志块，并在创建第21条日志记录时刷新到磁盘。
        // 其他15条日志记录保留在内存日志页面中，没有被刷新到磁盘上。
        printLogRecords("The log file now has these records:");
        // 对createRecords的第二个调用将创建记录36到70。
        createRecords(36, 70);
        // 调用flush告诉日志管理器确保记录65在磁盘上。但是由于记录66-70与记录65在同一内存页，因此它们也被写入磁盘。
        // 因此，对printLogRecords的第二次调用将以相反的顺序打印所有70条记录。
        lm.flush(65);
        printLogRecords("The log file now has these records:");
    }

    /**
     * 从磁盘的文件的块中一次读取一个块的内容到临时缓冲区的页
     * 读取块时是按照从最后一个块开始一直读取到第一个块结束
     * 创建一个page对象来包装日志记录，以便可以从记录中提取String和value。
     *
     * @param msg
     */
    private static void printLogRecords(String msg) {
        System.out.println(msg);
        // 为日志管理对象创建字节数组迭代器
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            // 将日志管理对象中的字节数组元素(就是一条一条的记录)取出放入字节数组中
            byte[] rec = iter.next();
            // 为这些记录的每一条新开一个临时的字节缓冲区的页
            Page p = new Page(rec);
            // 根据首部存储的字符串的字符个数取出字符串
            String s = p.getString(0);
            // 通过这些字符串的长度计算记录的int部分的位置
            int nPos = Page.maxLength(s.length());
            // 取出记录的int部分
            int val = p.getInt(nPos);
            System.out.println("[" + s + ", " + val + "]");
        }
        System.out.println();
    }

    /**
     * 测试用，创建(end - start)条日志信息存入各不相同的缓冲区
     *
     * @param start
     * @param end
     */
    private static void createRecords(int start, int end) {
        System.out.print("Creating records: ");
        for (int i = start; i <= end; i++) {
            byte[] rec = createLogRecord("记录" + i, i + 100);
            int lsn = lm.append(rec);
            System.out.print(lsn + " ");
        }
        System.out.println();
    }

    /**
     * 分配一个字节数组作为日志记录。
     * 它创建一个Page对象来包装该数组，以便可以使用页面的setInt和setString方法将字符串和整数放置在日志记录中的适当偏移量处。
     * 然后代码返回字节数组。
     *
     * @param s 要写入字节缓冲区的字符串
     * @param n 要写入字节缓冲区的int值
     * @return 返回装有(日志的字符串和信息的总长度)的字节数组
     */
    private static byte[] createLogRecord(String s, int n) {
        int sPos = 0;
        // 通过获取每条记录的长度转换为指定编码的字节流的长度来确定每条记录的在缓冲区的结束位置(即偏移量)
        int nPos = sPos + Page.maxLength(s.length());
        // 创建4+偏移量长度的字节数组
        byte[] b = new byte[nPos + Integer.BYTES];
        // 将字节数组包装成一个缓冲区的页
        Page p = new Page(b);
        // 在缓冲区的sPos位置写入字符串
        p.setString(sPos, s);
        // 在缓冲区的字符串之后写入一个4个字节的int值
        p.setInt(nPos, n);
        // 返回装有日志的字符串和信息的长度的字节数组
        return b;
    }
}
