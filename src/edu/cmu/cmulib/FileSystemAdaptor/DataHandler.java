package edu.cmu.cmulib.FileSystemAdaptor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by yiranfei on 4/9/15.
 */
public interface DataHandler<T> {
    public boolean ifFileExists(T fileSystem, String filePath) throws IOException;
    public DataInputStream getDataInputStream(T fileSystem, String filePath) throws IOException;
    public double[] getDataInDouble(T fileSystem, String filePath, int num) throws IOException;
    public DataOutputStream getDataOutputStream(T fileSystem, String filePath) throws IOException;
    public boolean writeDataOutDouble(T fileSystem, String filePath, double[] matrix) throws IOException;
}
