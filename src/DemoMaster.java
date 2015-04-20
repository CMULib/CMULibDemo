/**
 * Created by dongnanzhy on 4/14/15.
 */
import edu.cmu.cmulib.*;
import edu.cmu.cmulib.Utils.ConfParameter;
import edu.cmu.cmulib.Utils.JsonParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class DemoMaster {
    public static void main(String[] argv) throws IOException, ParseException {
        JsonParser jp = new JsonParser();
        ConfParameter conf = jp.parseFile(argv[0]);
        Master master = new Master(conf);
        master.init();

        while (true) {
            System.out.print("O");
            if (master.commu.slaveNum() >= conf.minSlaveNum)
                break;
        }

        String str = master.execute();
        System.out.println(str);
    }
}
