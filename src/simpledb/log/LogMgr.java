package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

/**
 * 日志管理器，它负责将日志记录写入日志文件。日志的尾部保存在字节缓冲区中，需要时将刷新到磁盘。
 * <p>
 * The log manager, which is responsible for
 * writing log records into a log file. The tail of
 * the log is kept in a bytebuffer, which is flushed
 * to disk when needed.
 *
 * @author Edward Sciore
 */
public class LogMgr {
    /**
     * 文件管理对象，由SimpleDB调用构造函数传入
     */
    private FileMgr fm;
    /**
     * 日志文件名称，由SimpleDB调用构造函数传入静态成员名称，始终确定为"simpledb.log"
     */
    private String logFile;
    /**
     * 日志页对象
     */
    private Page logPage;
    /**
     * 对磁盘上的目标文件的块的引用
     */
    private BlockId currentBlk;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    /**
     * 为指定的日志文件创建管理器。如果日志文件还不存在，则用空的第一个块创建它。
     *
     * @param fm      一个文件管理对象
     * @param logFile 日志文件名称
     */
    public LogMgr(FileMgr fm, String logFile) {
        this.fm = fm;
        this.logFile = logFile;
        byte[] b = new byte[fm.blockSize()];
        // 在内存创建与磁盘上的日志文件的块的大小(400)相同的  一个字节缓冲区，此缓冲区就为日志页
        logPage = new Page(b);
        // 获取磁盘上的日志文件当前总共拥有的块数
        int logSize = fm.length(logFile);
        // 如果文件只拥有1个块
        if (logSize == 0) {
            // 扩容日志文件并返回日志文件上当前正在进行写数据的块的对象
            currentBlk = appendNewBlock();
        } else {
            // 如果不止有1块
            // 获取日志文件的最后一个块的对象
            currentBlk = new BlockId(logFile, logSize - 1);
            // 将日志文件的最后一块的内容读入到缓冲区中的日志页上
            fm.read(currentBlk, logPage);
        }
    }

    /**
     * Ensures that the log record corresponding to the
     * specified LSN has been written to disk.
     * All earlier log records will also be written to disk.
     *
     * @param lsn the LSN of a log record
     */
    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    /**
     * 读取日志中的记录
     * 对迭代器的下一个方法的每次调用都将返回一个字节数组，表示日志中的下一条记录。
     * 迭代器方法返回的记录顺序相反，从最近的记录开始，在日志文件中向后移动。
     * 记录按此顺序返回，因为这是恢复管理器希望看到的。
     *
     * @return 返回日志记录的Java迭代器
     */
    public Iterator<byte[]> iterator() {
        // 将缓冲区中存在的内容强制输出到磁盘上
        // 第一次的话由于缓冲区刚在SimpleDB的作用下让logMgr new出来，没有内容
        flush();
        // 调用LogIterator的构造函数：传入缓冲区读写到磁盘的对象fm，以及磁盘上的文件的块的对象
        return new LogIterator(fm, currentBlk);
    }

    /**
     * 将日志记录追加到日志缓冲区。记录由任意字节数组组成。日志记录在缓冲区中从右到左写入。
     * 记录的大小在字节流之前写入。缓冲区的开始部分包含最后写入的记录的位置(“边界”)。
     * 把记录倒着存储，这样就很容易以相反的顺序读取它们。
     *
     * <p>
     * Appends a log record to the log buffer.
     * The record consists of an arbitrary array of bytes.
     * Log records are written right to left in the buffer.
     * The size of the record is written before the bytes.
     * The beginning of the buffer contains the location
     * of the last-written record (the "boundary").
     * Storing the records backwards makes it easy to read
     * them in reverse order.
     *
     * @param logRec 包含记录长度的字节数组。
     * @return 标识新的日志记录, 日志序列号(LSN) the LSN of the final value
     */
    public synchronized int append(byte[] logRec) {
        // 将缓冲区的日志页中的
        int boundary = logPage.getInt(0);
        // 获取日志的一条记录的长度
        int recSize = logRec.length;
        // 总共需要的长度是占位符加上日志的一条记录的本身长度
        int byteNeeded = recSize + Integer.BYTES;
        // 如果大小不能容纳
        if (boundary - byteNeeded < Integer.BYTES) {
            // 将缓冲区的内容强制写入到磁盘
            flush();
            // 重新在
            currentBlk = appendNewBlock();
            boundary = logPage.getInt(0);
        }
        int recPos = boundary - byteNeeded;

        try {
            logPage.setBytes(recPos, logRec);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // the new boundary
        logPage.setInt(0, recPos);
        latestLSN += 1;
        return latestLSN;
    }

    /**
     * 初始化字节流并将其附加到日志文件中。
     * <p>
     * Initialize the bytebuffer and append it to the log file.
     *
     * @return 磁盘上的日志文件的当前正在写入数据的块
     */
    private BlockId appendNewBlock() {
        // 扩展日志文件的大小
        BlockId blk = fm.append(logFile);
        // 在缓冲区的日志页的第0个字节开始写入四个字节的文件的块的大小(400)
        logPage.setInt(0, fm.blockSize());
        // 将日志页写入目标文件的目标块(此时是空块)中
        fm.write(blk, logPage);
        return blk;
    }

    /**
     * 调用flush方法将特定的日志记录强制保存到磁盘上。
     * 参数是日志记录的LSN;该方法确保将此日志记录(以及以前的所有日志记录)写入磁盘。
     * Write the buffer to the log file.
     */
    private void flush() {
        fm.write(currentBlk, logPage);
        lastSavedLSN = latestLSN;
    }
}
