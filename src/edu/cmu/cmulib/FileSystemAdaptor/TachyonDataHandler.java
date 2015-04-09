package edu.cmu.cmulib.FileSystemAdaptor;

import tachyon.TachyonURI;
import tachyon.client.FileInStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Created by yiranfei on 4/9/15.
 */
public class TachyonDataHandler implements DataHandler<TachyonFS> {
    @Override
    public boolean ifFileExists(TachyonFS fileSystem, String filePath) throws IOException {
        return fileSystem.getFile(new TachyonURI(filePath)) != null;
    }

    @Override
    public DataInputStream getDataInputStream(TachyonFS fileSystem, String filePath) throws IOException {
        TachyonFile file = fileSystem.getFile(new TachyonURI(filePath));
        if (file == null)
            return null;

        FileInStream in = new FileInStream(file, ReadType.CACHE);
        return new DataInputStream(in);

    }

    @Override
    public double[] getDataInDouble(TachyonFS fileSystem, String filePath, int num) throws IOException, EOFException {
        DataInputStream di = this.getDataInputStream(fileSystem, filePath);

        double[] data = new double[num];
        int ptr = 0;
        while (ptr < num) {
            data[ptr++] = di.readDouble();
        }

        return data;
    }
}

