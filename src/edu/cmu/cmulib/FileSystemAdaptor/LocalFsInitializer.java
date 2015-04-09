package edu.cmu.cmulib.FileSystemAdaptor;

import java.io.File;
import java.io.IOException;

/**
 * Created by yiranfei on 4/9/15.
 */
public class LocalFsInitializer implements FileSystemInitializer<String> {
    String mDirPath;

    public LocalFsInitializer() {
        mDirPath = null;
    }

    @Override
    public boolean connect(String dirPath) throws IOException {
        File theDir = new File(dirPath);
        if (!theDir.exists())
            return false;

        mDirPath = dirPath;
        return true;
    }

    @Override
    public boolean checkConnection() {
        return mDirPath == null;
    }

    @Override
    public String getFsHandler() {
        return mDirPath;
    }

    @Override
    public boolean disconnect() {
        mDirPath = null;
        return true;
    }
}
