package simpledb.file;

import simpledb.server.SimpleDB;

import java.io.IOException;

/**
 * @author Edward Sciore
 */
public class FileTest {
    public static void main(String[] args) throws IOException {
        // 三个参数指定引擎应该使用名为“studentdb”的数据库，使用400字节的块和8个缓冲区的池
        // 400字节的块大小是SimpleDB的默认值。
        // 在商业数据库系统中，该值将被设置为操作系统定义的块大小;一个典型的块大小是4K字节
        SimpleDB db = new SimpleDB("filetest", 400, 8);
        // 返回创建的对象
        FileMgr fm = db.fileMgr();
        BlockId blk = new BlockId("testfile", 2);
        int pos1 = 88;

        // 在文件“testfile”的逻辑地址的  第二块中的  第88个位置  写入字符串“abcdefghijklm”
        // 这里传入的其实是FileMgr的对象，处理与OS文件系统的实际交互
        // 这里的blockSize()是通过SimpleDB初始化时传入的blockSize进行传值的
        // 然后调用Page的构造方法创建直接字节缓冲区
        Page p1 = new Page(fm.blockSize());
        p1.setString(pos1, "abcdefghijklm");
        // 调用maxLength方法来确定字符串的最大长度，这样它就可以确定字符串后面的位置
        int size = Page.maxLength("abcdefghijklm".length());
        int pos2 = pos1 + size;
        // 将整数345写入该位置
        p1.setInt(pos2, 345);
        // 将指定块的内容写入指定的页
        fm.write(blk, p1);

        // 将此块读入另一个页面，并从中提取两个值。
        Page p2 = new Page(fm.blockSize());
        fm.read(blk, p2);
        System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
        System.out.println("offset " + pos1 + " contains " + p2.getString(pos1));
    }
}