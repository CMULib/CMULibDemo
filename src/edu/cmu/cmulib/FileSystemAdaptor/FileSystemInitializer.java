package edu.cmu.cmulib.FileSystemAdaptor;

import java.io.IOException;

/**
 * Created by yiranfei on 4/9/15.
 */
public interface FileSystemInitializer<T> {
    public boolean connect(String fsLocation) throws IOException;
    public boolean checkConnection();
    public T getFsHandler();
    public boolean disconnect();
}
