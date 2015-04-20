/**
 * Created by dongnanzhy on 4/14/15.
 */

import edu.cmu.cmulib.Slave;
import edu.cmu.cmulib.Utils.ConfParameter;
import edu.cmu.cmulib.Utils.JsonParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class DemoSlave {
    public static void main(String[] argv) throws IOException, ParseException {
        JsonParser jp = new JsonParser();
        ConfParameter conf = jp.parseFile(argv[0]);

        Slave slave = new Slave(conf);
        slave.init();
        do {
            slave.execute();
        } while(true);
    }
}
