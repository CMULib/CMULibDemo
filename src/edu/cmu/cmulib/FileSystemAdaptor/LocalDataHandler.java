package edu.cmu.cmulib.FileSystemAdaptor;

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
        di.close();
        return data;
    }

    public DataOutputStream getDataOutputStream(String fileSystem, String filePath) throws IOException{
        File file = new File(fileSystem + filePath);
        FileOutputStream out = new FileOutputStream(file);
        return new DataOutputStream(out);
    }
    public boolean writeDataOutDouble(String fileSystem, String filePath, double[] matrix) throws IOException{
        DataOutputStream di = this.getDataOutputStream(fileSystem, filePath);
        for (int i = 0; i < matrix.length; i++){
            di.writeDouble(matrix[i]);
        }
        di.close();
        return true;
    }
}
