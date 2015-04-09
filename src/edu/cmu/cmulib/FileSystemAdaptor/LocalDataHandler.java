package edu.cmu.cmulib.FileSystemAdaptor;

import tachyon.client.FileInStream;
import tachyon.client.ReadType;

import java.io.*;

/**
 * Created by yiranfei on 4/9/15.
 */
public class LocalDataHandler implements DataHandler<String> {

    @Override
    public boolean ifFileExists(String fileSystem, String filePath) throws IOException {
        File file = new File(fileSystem + filePath);

        return file.exists();
    }

    @Override
    public DataInputStream getDataInputStream(String fileSystem, String filePath) throws IOException {
        File file = new File(fileSystem + filePath);
        FileInputStream in = new FileInputStream(file);
        return new DataInputStream(in);
    }

    @Override
    public double[] getDataInDouble(String fileSystem, String filePath, int num) throws IOException {
        DataInputStream di = this.getDataInputStream(fileSystem, filePath);

        double[] data = new double[num];
        int ptr = 0;
        while (ptr < num) {
            data[ptr++] = di.readDouble();
        }

        return data;
    }
}
