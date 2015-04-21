import edu.cmu.cmulib.DistributedSVD;
import edu.cmu.cmulib.FileSystemAdaptor.*;
import edu.cmu.cmulib.MasterMiddleWare;
import edu.cmu.cmulib.Utils.KSVDconstant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;

/**
 * Created by kanghuang on 4/20/15.
 */
public class DemoKSVD {

    public static void main(String[] args) throws IOException, InterruptedException {
        // 4 slaves assumed
       // double[] test = new double[1000 * 1000];
        int row = KSVDconstant.rows;
        int col = KSVDconstant.cols;
        double[] test = new double[row * col];
        int q = 0;
        int slaveNum = 3;
        LinkedList<Double[]> mList = new LinkedList<Double[]>();

        //String dir = "tachyon://localhost:19998";
        //String fileName = "/BinData";
        String dir = "./resource";
        String fileName = "/BinData";
        int port = Integer.parseInt(args[0]);
        MasterMiddleWare commu = new MasterMiddleWare(port);
        commu.startMaster();
        int K = 3;
        int k = 0;
        while(k < K) {

            FileSystemInitializer fs = FileSystemAdaptorFactory.BuildFileSystemAdaptor(FileSystemType.LOCAL, dir);
            DataHandler tf = DataHandlerFactory.BuildDataHandler(FileSystemType.LOCAL);

            try {
//                test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
//                System.out.println(test[1000 * 1000 - 1]);
                test = tf.getDataInDouble(fs.getFsHandler(), fileName, row * col);
                System.out.println(test[row * col - 1]);
            } catch (IOException e) {
            }

            DistributedSVD svd = new DistributedSVD(commu, slaveNum, test);
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.err.println(k);
            while(k==0){
                System.err.println(k);
                if (br.readLine().equals("start")){
                    break;
                }
            }
            Thread t  = new Thread(svd);
            t.start();
            while (!t.getState().equals(Thread.State.TERMINATED)) {
                    //System.out.println((t.getState().equals(Thread.State.RUNNABLE)));

//                if (t!=null)
//                System.out.println(t.getState());
//                if (t != null && t.getState().equals(Thread.State.TERMINATED)) {
//                    break;
//                }
            }

            k++;
            System.err.println(k + " iteration finished");
        }
    }
}
