package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Page 的对象保存磁盘上的块的内容
 * 对内存的操作，与缓冲区有关
 * <p>
 * 作为ByteBuffer对象基础的字节数组可以来自Java数组，也可以来自操作系统的I/O缓冲区。
 * Page类有两个构造函数，每个构造函数对应于一种不同类型的底层字节数组。
 * <p>
 * 各种get和set方法使客户机能够存储或访问页的指定位置上的值
 *
 * @author Edward Sciore
 */
public class Page {
    private ByteBuffer bb;
    /**
     * 字符串与其字节表示形式之间的转换由字符编码决定。
     * String的构造函数及其getBytes方法采用字符集参数。
     */
    public static Charset CHARSET = StandardCharsets.UTF_8;

    /**
     * 在内存上创建一个大小为blocksize的直接缓冲区
     *
     * @param blocksize 直接缓冲区的大小
     */
    public Page(int blocksize) {
        // 通过调用此类的allocateDirect工厂方法来创建直接字节缓冲区
        // 字节缓冲区是直接 或非直接的 。
        // 给定一个直接字节缓冲区，Java虚拟机将尽力在其上直接执行本地I / O操作。
        // 也就是说，它将尝试避免在每次调用其中一个底层操作系统的本机I / O操作之前（或之后）将缓冲区的内容复制到（或从）中间缓冲区。
        // 新缓冲区的位置将为零，其限制将为其容量，其标记将不定义，并且其每个元素将被初始化为零。 它是否有后备数组未指定。
        bb = ByteBuffer.allocateDirect(blocksize);
    }

    /**
     * 创建一个从Java数组获取内存的页;
     * 这个构造函数主要由日志管理器使用
     * <p>
     * For creating log pages
     *
     * @param b
     */
    public Page(byte[] b) {
        // wrap(byte[] array)
        // 将一个字节数组包装到缓冲区中。
        bb = ByteBuffer.wrap(b);
    }

    /**
     * 此方法读取缓冲区中的int时不用读取int的长度
     *
     * @param offset 要读取内容在缓冲区的位置
     * @return 要读取的int
     */
    public int getInt(int offset) {
        // public abstract int getInt(int index)
        // 用于读取int值的绝对get方法。
        // 在给定索引处读取四个字节，根据当前字节顺序将它们组合成一个int值。
        return bb.getInt(offset);
    }

    /**
     * 以给定的索引将包含给定int值的四个字节以当前字节顺序写入此缓冲区。
     *
     * @param offset 要写入数据的位置
     * @param n      要写入的int值
     */
    public void setInt(int offset, int n) {
        // putInt(int index, int value)
        // 用于写入int值的绝对 put方法 （可选操作）
        bb.putInt(offset, n);
    }

    /**
     * 读取缓冲区中字符串二进制化后的内容
     *
     * @param offset
     * @return
     */
    public byte[] getBytes(int offset) {
        bb.position(offset);
        // 读取字符串的长度，用来恢复二进制化的字符串
        int length = bb.getInt();
        byte[] b = new byte[length];
        bb.get(b);
        return b;
    }

    /**
     * ByteBuffer类没有读写字符串的方法，因此Page选择将字符串值写成blob。
     * <p>
     * 将blob保存为两个值:首先是指定blob中的字节数，然后是字节本身。
     * <p>
     * 调用ByteBuffer的putInt方法来写入4个字节的传入缓冲区的内容的长度(文件头)，调用put方法来写入文件
     *
     * @param offset
     * @param b
     */
    public void setBytes(int offset, byte[] b) {
        // position(int newPosition)
        // 设置这个缓冲区的位置。
        bb.position(offset);
        // putInt(int value)
        // 编写int值的相对 put方法 （可选操作） 。
        // 这里把字节流的长度 用4个字节的长度写入了缓冲区 这些数据的作用就是索引
        bb.putInt(b.length);
        // put(byte b)
        // 相对 放置法 （可选操作） 。
        bb.put(b);
    }

    /**
     * 返回内存的指定缓冲区的位置的字符串
     * 从字节缓冲区读取blob，然后将字节转换为字符串。
     *
     * @param offset 要读取字符串的起始位置
     * @return 内存的指定缓冲区的位置的字符串
     */
    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    /**
     * 调用getBytes将字符串转换为字节，然后将这些字节作为blob写入内存中的缓冲区
     * 此方法读取缓冲区中的字符串时需要读取缓冲区中字符串的头(头中有字符串二进制化后的长度)
     *
     * @param offset
     * @param s
     */
    public void setString(int offset, String s) {
        // 使用给定的charset将该String编码为字节序列，将结果存储到新的字节数组中。
        byte[] b = s.getBytes(CHARSET);
        // 调用Page.setBytes()
        setBytes(offset, b);
    }

    /**
     * 字符集选择每个字符编码的字节数。
     * ASCII对每个字符使用一个字节，而Unicode-16对每个字符使用2到4字节。
     * 因此，数据库引擎可能不知道给定字符串将编码为多少字节。
     * <p>
     * 头4个字节是4个字节的占位符,之后是strlen长度的UTF-8的字符串所转换成二进制需要的长度
     *
     * @param strlen
     * @return
     */
    public static int maxLength(int strlen) {
        // newEncoder()
        // 为此字符集构造一个新的编码器。
        // maxBytesPerChar()
        // 返回为每个输入字符产生的最大字节数。这个值是根据字符编码格式来的固定值，UTF-8是3
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        // BYTES
        // 用于表示二进制补码二进制形式的 int值的字节数。
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    // a package private method, needed by FileMgr

    /**
     * 将这个缓冲区的指针位置设置为0
     *
     * @return ByteBuffer对象
     */
    ByteBuffer contents() {
        // position(int newPosition)
        // 设置这个缓冲区的位置。
        bb.position(0);
        return bb;
    }
}
