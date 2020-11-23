package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * 实现将页面读写到磁盘块的方法。
 * <p>
 * 文件管理器始终从文件中读取或写入块大小的字节数，并且始终处于块边界。
 * 在此过程中，文件管理器确保对读、写或追加的每次调用将导致一次磁盘访问。
 *
 * @author Edward Sciore
 */
public class FileMgr {
    private File dbDirectory;
    /**
     * 磁盘上的文件的块的大小
     */
    private int blocksize;
    private boolean isNew;
    private static int readBlockCount;
    private static int writeBlockCount;
    /**
     * openFiles 一个HashMap，键是文件名， 值是文件随机读取权限
     */
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    /**
     * 数据库名称用作包含数据库文件的文件夹的名称;此文件夹位于引擎的当前目录中。
     * 如果不存在这样的文件夹，则为新数据库创建一个文件夹
     *
     * @param dbDirectory 数据库名称的字符串
     * @param blocksize   每个块大小的整数
     */
    public FileMgr(File dbDirectory, int blocksize) {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        // exists() 测试此抽象路径名表示的文件或目录是否存在。
        isNew = !dbDirectory.exists();

        // create the directory if the database is new
        if (isNew) {
            dbDirectory.mkdirs();
        }

        // remove any leftover temporary tables(删除所有剩余的临时表)
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                // File(File parent, String child)  从父抽象路径名和子路径名字符串创建新的 File实例。
                new File(dbDirectory, filename).delete();
            }
        }
    }

    /**
     * 将磁盘上的文件的  指定块号的块  的内容读入指定的缓冲区位置
     *
     * @param blk 指定的磁盘上的文件的块的对象
     * @param p   指定的内存上的缓冲区的对象
     */
    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            // 从随机读写文件的通道读取内容到给定缓冲区
            f.getChannel().read(p.contents());
            countReadBlockNums();
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    /**
     * 将指定的缓冲区位置  的内容写入 磁盘上的文件的  指定块号的块
     *
     * @param blk 指定的磁盘上的文件的块的对象
     * @param p   指定的内存上的缓冲区的对象
     */
    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            // 将磁盘上的文件内的块的指针移动到指定的块上(块号*块的大小)
            f.seek(blk.number() * blocksize);
            // getChannel()
            // 返回与此文件关联的唯一的FileChannel(用于读取，写入，映射和操作文件的通道。)对象。
            // write(ByteBuffer src)
            // 从给定的缓冲区位置向 写文件通道 写入一个字节序列。
            // 从给定的要写入的磁盘中的文件的块 的第一个字节开始写入，而不是紧跟着块中已有的数据的后方写入
            // 除非该通道处于append模式

            // 从磁盘上的文件打开一个通道，该通道执行的是从内存的缓冲区写入字节到磁盘，把内存的缓冲区的指针指向缓冲区开头
            f.getChannel().write(p.contents());
            countWriteBlockNums();
        } catch (IOException e) {
            throw new RuntimeException("cannot write block" + blk);
        }
    }

    /**
     * 寻找文件的末尾，并向其写入一个空的字节数组，这将导致OS自动扩展该文件。
     *
     * @param filename
     * @return
     */
    public synchronized BlockId append(String filename) {
        int newblknum = length(filename);
        BlockId blk = new BlockId(filename, newblknum);
        byte[] b = new byte[blocksize];
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int) (f.length() / blocksize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blocksize;
    }

    /**
     * 根据传入的文件名，获取磁盘上文件的随机读写权限，如果没有该文件则创建并赋予随机读写权限
     *
     * @param filename 磁盘上需要获取随机读写权限的文件的文件名
     * @return 可随机读写的文件对象
     * @throws IOException IO异常
     */
    private RandomAccessFile getFile(String filename) throws IOException {
        // 获取在HashMap中的文件的随机读写权限
        RandomAccessFile f = openFiles.get(filename);
        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }

    /**
     *
     * @return  本次操作对文件的读取情况
     */
    public int countReadBlockNums() {
        readBlockCount++;
        //System.out.println("读了" + readBlockCount + "块磁盘上的文件的块到内存缓冲区");
        return readBlockCount;
    }

    /**
     *
     * @return  本次操作对文件的写入情况
     */
    public int countWriteBlockNums() {
        writeBlockCount++;
        //System.out.println("写了" + writeBlockCount + "页内存缓冲区的文件到磁盘上的文件的块");
        return writeBlockCount;
    }
}
