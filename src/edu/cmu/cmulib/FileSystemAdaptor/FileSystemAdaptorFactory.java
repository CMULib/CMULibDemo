package edu.cmu.cmulib.FileSystemAdaptor;

import java.io.IOException;

/**
 * Created by yiranfei on 4/18/15.
 */
public class FileSystemAdaptorFactory {
    public static FileSystemInitializer BuildFileSystemAdaptor(FileSystemType type, String dir) throws IOException {
        FileSystemInitializer adaptor = null;

        switch (type) {
            case TACHYON:
                adaptor = new TachyonInitialier();
                adaptor.connect(dir);
                break;

            case LOCAL:
                adaptor = new LocalFsInitializer();
                adaptor.connect(dir);
                break;
        }

        return adaptor;
    }
}
