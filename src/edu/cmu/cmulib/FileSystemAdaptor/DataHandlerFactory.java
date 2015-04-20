package edu.cmu.cmulib.FileSystemAdaptor;

import java.io.IOException;

/**
 * Created by yiranfei on 4/18/15.
 */
public class DataHandlerFactory {
    public static DataHandler BuildDataHandler(FileSystemType type) throws IOException {
        DataHandler adaptor = null;

        switch (type) {
            case TACHYON:
                adaptor = new TachyonDataHandler();
                break;

            case LOCAL:
                adaptor = new LocalDataHandler();
                break;
        }

        return adaptor;
    }
}
