package simpledb.file;
import java.io.*;

/**
 * @author Edward Sciore
 */
public class RandomAccessFileTest {
   public static void main(String[] args) throws IOException {
      File file = new File("testfile");            
      try {
         // initialize the file
         // “rws”开放读写，并且还要求对文件内容或元数据的每次更新都同步写入底层存储设备。
         /**
          * 文件是在“rws”模式下打开的。
          * “rw”部分指定文件已打开以便进行读写。
          * <p>
          * “s”部分指定操作系统不应该延迟磁盘I/O以优化磁盘性能;相反，每个写操作都必须立即写入磁盘。
          * 这个特性可以确保数据库引擎准确地知道磁盘写操作何时发生，这对于实现第5章中的数据恢复算法尤其重要。
          */
         RandomAccessFile f1 = new RandomAccessFile(file, "rws");
         // 指针指向123这个位置
         f1.seek(123);
         // 在123这个位置写入32位int的999
         f1.writeInt(999);
         // 文件流关闭
         f1.close();

         // increment the file
         RandomAccessFile f2 = new RandomAccessFile(file, "rws");
         // 指针指向123这个位置
         f2.seek(123);
         // 读取32位的一个int整数
         int n = f2.readInt();
         // 再次将指针指向123这个位置
         f2.seek(123);
         // 在这个位置写入32位int的1000
         f2.writeInt(n+1);
         // 关闭文件流
         f2.close();

         // re-read the file
         RandomAccessFile f3 = new RandomAccessFile(file, "rws");
         // 指针指向123这个位置
         f3.seek(123);
         // 输出读取到的32位的一个int整数
         System.out.println("The new value is " + f3.readInt());
         f3.close();
      }
      catch (IOException e) {
         e.printStackTrace();
      }
   }
}