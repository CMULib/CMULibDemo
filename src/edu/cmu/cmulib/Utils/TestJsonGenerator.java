package edu.cmu.cmulib.Utils;

import edu.cmu.cmulib.FileSystemAdaptor.FileSystemType;
import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by yiranfei on 4/18/15.
 */
public class TestJsonGenerator {
    public static void main(String[] argv) throws IOException {
        JSONObject obj = new JSONObject();
        obj.put("masterAddress", "localhost");
        obj.put("masterPort", new Integer(8888));
        obj.put("minSlaveNum", new Integer(1));
        obj.put("fsType", "local");
        obj.put("fileDir", "./resource");
        obj.put("fileName", "/BinData");
        System.out.print(obj);

        FileWriter file = new FileWriter("./resource/conf.json");
        file.write(obj.toJSONString());
        file.flush();
        file.close();
    }
}
