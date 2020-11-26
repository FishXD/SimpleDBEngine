package simpledb.file;

import simpledb.server.SimpleDB;

import java.io.IOException;

/**
 * @author Edward Sciore
 */
public class FileTest {
    public static void main(String[] args) throws IOException {
        // 三个参数指定引擎应该使用名为“studentdb”的数据库，使用400字节的块和具有8个的缓冲区插槽的缓冲区管理器
        // 400字节的块大小是SimpleDB的默认值。
        // 在商业数据库系统中，该值将被设置为操作系统定义的块大小;一个典型的块大小是4K字节
        SimpleDB db = new SimpleDB("filetest", 400, 8);
        FileMgr fm = db.fileMgr();
        BlockId blk = new BlockId("testfile", 2);
        int pos1 = 88;

        // 这里的blockSize()是通过SimpleDB初始化时传入的blockSize进行传值的
        // 然后调用Page的构造方法在内存中创建一个大小为blockSize(这里是400)的直接字节缓冲区
        // 这个缓冲区是用来从把内容写入磁盘
        Page p1 = new Page(fm.blockSize());
        // 在p1创建的字节缓冲区的第88个位置  写入转换为二进制的字符串“abcdefghijklm”  的长度(4个字节的int)及其二进制化的本身内容
//        String str = "3.13. In UNIX, the directory entry for a file points to a block called an inode. In one implementation of an inode, the beginning of the block holds various header information, and its last 60 bytes contains 15 integers. The first 12 of these integers are the physical locations of the first 12 data blocks in the file. The next two integers are the location of two index blocks, and the last integer is the location of a double-index block. An index block consists entirely of block numbers of the next data blocks in the file; a double-index block consists entirely of block numbers of index blocks (whose contents point to data blocks).";
//        p1.setString(pos1, str);
//        int size = Page.maxLength(str.length());
        p1.setStringForChar(pos1, "abcdefghijklm");
//        // 调用maxLength方法来确定这个字符集(UTF-8)的字符串转换为二进制的最大长度，这样它就可以确定字符串后面的位置
        int size = Page.maxLength("abcdefghijklm".length());
//        p1.setDate(pos1, "2020-11-25 22:21:17:691");
//        int size = Page.maxLength("2020-11-25 22:21:17:691".length());
        int pos2 = pos1 + size;
        // 将整数345紧挨在  转换为二进制的字符串后方缓冲区  然后写入

        p1.setInt(pos2, 345);

        // 将内存上的p1缓冲区的内容写入到磁盘上fm对象所指定的块
        fm.write(blk, p1);

        // 将此块读入另一个页面，并从中提取两个值。
        // 调用Page的构造方法在内存中创建一个大小为blockSize(这里是400)的直接字节缓冲区
        // 这个缓冲区是用来从把内容从磁盘读入内存的缓冲区
        Page p2 = new Page(fm.blockSize());
        // 将磁盘上fm对象所指定的块写入到内存上的p2缓冲区的内容(缓冲区读取磁盘内容)
        fm.read(blk, p2);
        // 在内存上的p2缓冲区的pos2处读取4个字节

        System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
        System.out.println("offset " + pos1 + " contains " + p2.getStringForChar(pos1));
    }
}