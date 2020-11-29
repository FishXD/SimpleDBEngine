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
    private int blockSize;
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
     * @param dbDirectory 要管理数据库目录文件的对象
     * @param blockSize   磁盘上的块的大小
     */
    public FileMgr(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        // exists() 测试此抽象路径名表示的文件或目录是否存在。
        isNew = !dbDirectory.exists();

        // create the directory if the database is new
        if (isNew) {
            dbDirectory.mkdirs();
        }

        // remove any leftover temporary tables(删除所有剩余的临时表)
        for (String fileName : dbDirectory.list()) {
            if (fileName.startsWith("temp")) {
                // File(File parent, String child)  从父抽象路径名和子路径名字符串创建新的 File实例。
                // 删除由此抽象路径名表示的文件或目录。如果此路径名表示一个目录，则要删除该目录，该目录必须为空。
                new File(dbDirectory, fileName).delete();
            }
        }
    }

    /**
     * 将磁盘上的文件的  指定块号的块  的内容读入指定的缓冲区位置
     *
     * @param blk 出发磁盘块
     * @param p   目的内存页
     */
    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
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
            f.seek(blk.number() * blockSize);
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
     * @param fileName  需要寻找末尾并在末尾写入空的字节数组以此来扩展文件块数的文件的文件名
     * @return  返回文件的目标块的对象
     */
    public synchronized BlockId append(String fileName) {
        // 通过length方法获取到该块在文件的第几块中
        int newBlkNum = length(fileName);
        // 获取文件的在目标块的对象
        BlockId blk = new BlockId(fileName, newBlkNum);
        byte[] b = new byte[blockSize];
        try {
            // 获取磁盘上的目标文件的随机读写权限
            RandomAccessFile f = getFile(blk.fileName());
            // 将目标文件的块指针移到目标块上
            f.seek(blk.number() * blockSize);
            // 在目标文件中的目标块上写入400字节的blockSize的字节数组，致使OS自动扩展该文件的块数
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    /**
     * 根据传入的文件名判断该文件需要几个完整的内存页来放置。注意：文件的块号从0开始。
     * <p>
     *     如文件名为"1.txt"的文件长500字节,1个内存页400字节，则调用此方法返回1。
     *
     * @param fileName  需要与内存页大小比较的文件的文件名
     * @return 返回该文件需要几个完整的内存页来放置，注意：文件的块号从0开始。
     */
    public int length(String fileName) {
        try {
            RandomAccessFile f = getFile(fileName);
            return (int) (f.length() / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + fileName);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blockSize;
    }

    /**
     * 根据传入的文件名，获取磁盘上文件的随机读写权限，如果没有该文件则创建并赋予随机读写权限
     *
     * @param fileName 磁盘上需要获取随机读写权限的文件的文件名
     * @return 可随机读写的文件对象
     * @throws IOException IO异常
     */
    private RandomAccessFile getFile(String fileName) throws IOException {
        // 获取在HashMap中的文件的随机读写权限
        RandomAccessFile f = openFiles.get(fileName);
        if (f == null) {
            File dbTable = new File(dbDirectory, fileName);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(fileName, f);
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
