package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 *
 * @author Edward Sciore
 */
class LogIterator implements Iterator<byte[]> {
    private FileMgr fm;
    private BlockId blk;
    private Page p;
    private int currentPos;
    private int boundary;

    /**
     * 为日志文件中的记录创建迭代器，位于最后一条日志记录之后。
     * <p>
     * Creates an iterator for the records in the log file,
     * positioned after the last log record.
     */
    public LogIterator(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.blk = blk;
        // 创建一个大小为400的字节数组
        byte[] b = new byte[fm.blockSize()];
        // 在缓冲区创建一个大小为400的字节数组的页，这个页并不是第一个被创建出来的页，
        p = new Page(b);
        moveToBlock(blk);
    }

    /**
     * Determines if the current log record
     * is the earliest record in the log file.
     *
     * @return true if there is an earlier record
     */
    @Override
    public boolean hasNext() {
        return currentPos < fm.blockSize() || blk.number() > 0;
    }

    /**
     * 在块中从左到右取记录，直到存储最新记录的块的记录被取完，指针移到倒数第二新的块。
     * 移动到日志块中的下一个日志记录。如果块中没有更多的日志记录，那么移到前一个块并从那里返回日志记录。
     * 此方法剥去存储记录长度的int首部
     * <p>
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block
     * and return the log record from there.
     *
     * @return 只包含记录长度的int首部与记录(the next earliest log record)
     */
    @Override
    public byte[] next() {
        // 在块中从左到右取记录，直到存储最新记录的块的记录被取完，指针移到倒数第二新的块
        if (currentPos == fm.blockSize()) {
            blk = new BlockId(blk.fileName(), blk.number() - 1);
            moveToBlock(blk);
        }
        // 从currentPos位置开始取出存储记录长度的int首部，并根据这个首部取出整个记录转换成的字节数组
        byte[] rec = p.getBytes(currentPos);
        // 指针移动到下一条记录的int首部
        currentPos += Integer.BYTES + rec.length;
        return rec;
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in that block
     * (i.e., the most recent one).
     */
    private void moveToBlock(BlockId blk) {
        fm.read(blk, p);
        boundary = p.getInt(0);
        currentPos = boundary;
    }
}
