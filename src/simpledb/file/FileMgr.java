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
    private int blocksize;
    private boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    /**
     * 数据库名称用作包含数据库文件的文件夹的名称;此文件夹位于引擎的当前目录中。
     * 如果不存在这样的文件夹，则为新数据库创建一个文件夹
     *
     * @param dbDirectory 数据库名称的字符串
     * @param blocksize 每个块大小的整数
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
     * 查找指定文件中的适当位置，并将该块的内容读入指定页的字节缓冲区。
     *
     * @param blk
     * @param p
     */
    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    /**
     * 将指定块的内容写入指定的页
     *
     * @param blk
     * @param p
     */
    public synchronized void write(BlockId blk, Page p) {
        try {
            // 通过传入的块知道块是在哪个文件中的
            // 再通过文件名获取文件的对象
            RandomAccessFile f = getFile(blk.fileName());
            // 将文件的指针移动到指定的字节上(块号*块的大小)
            f.seek(blk.number() * blocksize);
            // getChannel()
            // 返回与此文件关联的唯一的FileChannel(用于读取，写入，映射和操作文件的通道。)对象。
            // write(ByteBuffer src)
            // 从给定的缓冲区向该通道写入一个字节序列。
            //
            f.getChannel().write(p.contents());
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

    private RandomAccessFile getFile(String filename) throws IOException {
        // 返回指定键映射到的值，如果该映射不包含该键的映射，则返回null。
        RandomAccessFile f = openFiles.get(filename);
        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }
}
