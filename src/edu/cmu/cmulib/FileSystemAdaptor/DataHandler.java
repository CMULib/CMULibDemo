package edu.cmu.cmulib.FileSystemAdaptor;

import tachyon.client.TachyonFS;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Created by yiranfei on 4/9/15.
 */
public interface DataHandler<T> {
    public boolean ifFileExists(T fileSystem, String filePath) throws IOException;
    public DataInputStream getDataInputStream(T fileSystem, String filePath) throws IOException;
    public double[] getDataInDouble(T fileSystem, String filePath, int num) throws IOException;
}
