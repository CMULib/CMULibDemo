package edu.cmu.cmulib.FileSystemAdaptor;

import tachyon.TachyonURI;
import tachyon.client.*;

import java.io.IOException;

/**
 * Created by yiranfei on 4/9/15.
 */
public class TachyonInitialier implements FileSystemInitializer<TachyonFS> {
    TachyonURI mMasterLocation;
    TachyonFS mTachyonClient;

    public TachyonInitialier() {
        mMasterLocation = null;
        mTachyonClient = null;
    }

    @Override
    public boolean connect(String fsLocation) throws IOException {
        mMasterLocation = new TachyonURI(fsLocation);
        mTachyonClient = TachyonFS.get(mMasterLocation);

        if (mMasterLocation == null)
            return false;

        return true;
    }

    @Override
    // TODO(fyraimar) replace fake implementation
    public boolean checkConnection() {
        return mMasterLocation != null;
    }

    @Override
    public TachyonFS getFsHandler() {
        if (this.mTachyonClient instanceof TachyonFS)
            return this.mTachyonClient;
        else
            return null;
    }

    @Override
    public boolean disconnect() {
        return false;
    }
}
