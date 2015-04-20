package edu.cmu.cmulib.Utils;

import edu.cmu.cmulib.FileSystemAdaptor.FileSystemType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by yiranfei on 4/18/15.
 */
public class JsonParser {
    ConfParameter parameters = null;

    public JsonParser() {
    }

    public ConfParameter parseFile(String filePath) throws IOException, ParseException {
        ConfParameter conf = new ConfParameter();

        FileReader reader = new FileReader(filePath);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);

        conf.masterAddress = (String) jsonObject.get("masterAddress");
        conf.masterPort = ((Long) jsonObject.get("masterPort")).intValue();
        conf.minSlaveNum = ((Long) jsonObject.get("minSlaveNum")).intValue();
        conf.fileDir = (String) jsonObject.get("fileDir");
        conf.fileName = (String) jsonObject.get("fileName");

        String fs = (String) jsonObject.get("fsType");
        // In order to support jdk 1.6, use if-else instead of switch (String).
        if (fs.toLowerCase().equals("tachyon")) {
            conf.fsType = FileSystemType.TACHYON;
        } else if (fs.toLowerCase().equals("local")) {
            conf.fsType = FileSystemType.LOCAL;
        }

        return conf;
    }

    public ConfParameter parseString(String para) throws ParseException {
        ConfParameter conf = new ConfParameter();
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(para);

        conf.masterAddress = (String) jsonObject.get("masterAddress");
        conf.masterPort = ((Long) jsonObject.get("masterPort")).intValue();
        conf.minSlaveNum = ((Long) jsonObject.get("minSlaveNum")).intValue();
        conf.fileDir = (String) jsonObject.get("fileDir");
        conf.fileName = (String) jsonObject.get("fileName");

        String fs = (String) jsonObject.get("fsType");
        // In order to support jdk 1.6, use if-else instead of switch (String).
        if (fs.toLowerCase().equals("tachyon")) {
            conf.fsType = FileSystemType.TACHYON;
        } else if (fs.toLowerCase().equals("local")) {
            conf.fsType = FileSystemType.LOCAL;
        }

        return conf;
    }

    public static void main(String[] argv) throws IOException, ParseException {
        String filename = "./resource/conf.json";

        JsonParser jp = new JsonParser();
        ConfParameter cf = jp.parseFile(filename);

        System.out.println(cf.masterAddress);
        System.out.println(cf.masterPort);
        System.out.println(cf.minSlaveNum);
        System.out.println(cf.fsType);
        System.out.println(cf.fileDir);
        System.out.println(cf.fileName);
    }
}
