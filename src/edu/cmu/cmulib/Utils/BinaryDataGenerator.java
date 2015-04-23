package edu.cmu.cmulib.Utils;

import java.io.*;

/**
 * Created by yiranfei on 4/6/15.
 */
public class BinaryDataGenerator {
  public void write(double data[]) throws IOException {
    DataOutputStream out = new DataOutputStream(new FileOutputStream("./resource/BinData"));
    for (int i = 0; i < 1000 * 1000; i++) {
      out.writeDouble(data[i]);
    }
    out.close();
  }

  public void readtest(String filePath) throws IOException {
    DataInputStream in = new DataInputStream(new FileInputStream(filePath));
    double[] test = new double[1000*1000];
    int q = 0;
    while(in.available() > 0)
    {
      test[q] = in.readDouble();
      q++;
    }

    System.out.println(test[0]);
  }

  public double[] read(String filePath) throws IOException {
    BufferedReader br = null;
    double[] test = new double[1000*1000];
    int q = 0;
    try {
      br = new BufferedReader(new FileReader(filePath));

      String line;
      while ((line = br.readLine()) != null) {
        test[q] = Double.parseDouble(line);
        q++;
      }
      br.close();
      //DataOutputStream out = new DataOutputStream(new FileOutputStream("MyBinaryFile.txt"));
      //out.writeDouble(double d);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }


    return test;
  }

  public static void main(String argv[]) {
    BinaryDataGenerator g = new BinaryDataGenerator();
    try {
      double[] data = g.read("./resource/svd.data.txt");
      //double[] data = g.read("./resource/data.txt");
      g.write(data);
      //g.readtest("/Users/yiranfei/Desktop/BinData");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
