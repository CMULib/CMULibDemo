/**
 * Created by dongnanzhy on 4/14/15.
 */
import edu.cmu.cmulib.*;

import java.io.IOException;

public class Demo {
    public static void main(String[] args) throws IOException {
        Master master = new Master();
        master.init();
        do {
            String str = master.execute();
            System.out.println(str);
        } while(!master.isCompleted());
        String finalRst = master.dispFinal();
        System.out.println(finalRst);
    }
}
