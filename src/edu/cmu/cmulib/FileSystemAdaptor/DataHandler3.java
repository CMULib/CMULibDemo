/**
 * Created by yiranfei on 4/4/15.
 */
package edu.cmu.cmulib.FileSystemAdaptor;

import tachyon.TachyonURI;
import tachyon.client.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DataHandler3 {
  TachyonURI mMasterLocation;
  TachyonURI mFilePath;
  TachyonFS mTachyonClient;
  WriteType mWriteByte = WriteType.CACHE_THROUGH;

  public DataHandler3(String masterLocation, String filePath) throws IOException {
    mMasterLocation = new TachyonURI(masterLocation);
    mFilePath = new TachyonURI(filePath);
    mTachyonClient = TachyonFS.get(mMasterLocation);
  }

  public void createFile() throws IOException {
    mTachyonClient.createFile(mFilePath);
  }

  public void writeFile(int number) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(number * 4);
    buf.order(ByteOrder.nativeOrder());
    for (int k = 0; k < number; k ++) {
      buf.putInt(k);
    }

    buf.flip();
    buf.flip();

    TachyonFile file = mTachyonClient.getFile(mFilePath);
    OutStream os = file.getOutStream(mWriteByte);
    os.write(buf.array());
    os.close();
  }
/*
  public static void main(String argv[]) {
    String masterLocation = "tachyon://localhost:19998";
    String filePath = "/BinData";
    try {
      DataHandler t = new DataHandler(masterLocation, filePath);
      //t.createFile();
      //t.writeFile(10);
      //System.out.print(t.readFile(9));
      t.readData();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
*/
  private int readTestFile(int number) throws IOException {
    TachyonFile file = mTachyonClient.getFile(mFilePath);
    TachyonByteBuffer buf = file.readByteBuffer(0);
    if (buf == null) {
      file.recache();
      buf = file.readByteBuffer(0);
    }
    buf.mData.order(ByteOrder.nativeOrder());

    int sum = 0;
    for (int k = 0; k < number; k ++) {
      sum += buf.mData.getInt();
    }
    buf.close();

    return sum;
  }

  public double[] readData() throws IOException {
    TachyonFile file = mTachyonClient.getFile(mFilePath);
    FileInStream in = new FileInStream(file, ReadType.CACHE);

    double[] data = new double[1000 * 1000];
    int ptr = 0;

    DataInputStream di = new DataInputStream(in);
    while (ptr < 1000 * 1000) {
      data[ptr++] = di.readDouble();

    }
    //System.out.println(data[1000 * 1000 -1]);

    return data;
  }
}
