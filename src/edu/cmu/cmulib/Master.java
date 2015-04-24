package edu.cmu.cmulib;

import edu.cmu.cmulib.Communication.CommonPacket;
import edu.cmu.cmulib.CoolMatrixUtility.core.Mat;
import edu.cmu.cmulib.CoolMatrixUtility.core.MatOp;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Master_SVD;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Master_Spliter;
import edu.cmu.cmulib.CoolMatrixUtility.help.Tag;
import edu.cmu.cmulib.FileSystemAdaptor.*;
import edu.cmu.cmulib.Utils.ConfParameter;
import edu.cmu.cmulib.Utils.JsonParser;
import edu.cmu.cmulib.Utils.KSVDconstant;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;

public class Master {
    private Mat score;
    private Mat Like;
    private Master_Spliter split;
    private Master_SVD svd;
    private MasterMiddleWare commu;
    private LinkedList<Double[]> mList;
    public int slaveNum;
    public String dir;
    public String fileName;
    public int port;
    public FileSystemType mFsType;

    public Master() {
        this.slaveNum = 4;
        this.dir = "./resource";
        this.fileName = "/BinData";
        this.port = 8888;
        this.mFsType = FileSystemType.LOCAL;
    }

    public Master(ConfParameter conf) {
        this.slaveNum = conf.minSlaveNum;
        this.dir = conf.fileDir;
        this.fileName = conf.fileName;
        this.port = conf.masterPort;
        this.mFsType = conf.fsType;
    }

    public Master(String filePath) throws IOException, ParseException {
        JsonParser jp = new JsonParser();
        ConfParameter conf = jp.parseFile(filePath);

        this.slaveNum = conf.minSlaveNum;
        this.dir = conf.fileDir;
        this.fileName = conf.fileName;
        this.port = conf.masterPort;
        this.mFsType = conf.fsType;
    }

    public String sayHi() {
        return "HHHHHHHHHHHHHH";
    }

    public void init() throws IOException {
        //double[] test = new double[1000*1000];
        double[] test = new double[8*4];
        mList = new LinkedList<Double[]>();
        try {
            FileSystemInitializer fs = FileSystemAdaptorFactory.BuildFileSystemAdaptor(mFsType, dir);
            DataHandler t = DataHandlerFactory.BuildDataHandler(mFsType);
            //test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
            test = t.getDataInDouble(fs.getFsHandler(), fileName, 8 * 4);
            //System.out.println(test[1000 * 1000 - 1]);
            System.out.println(test[8*4 - 1]);
        } catch (IOException e) {
        }

        // initialize original matrix
//        int rows = 1000;
//        int cols = 1000;
        int rows = 8;
        int cols = 4;
        this.score = new Mat(rows, cols ,test);

        commu = new MasterMiddleWare(port);
        commu.register(Double[].class, mList);
        commu.startMaster();

        this.split = new Master_Spliter(score, slaveNum);
        this.svd = new Master_SVD(score, slaveNum);
        this.Like = svd.initL();
        while(commu.slaveNum()<slaveNum){System.out.println(commu.slaveNum());}
    }

    public String execute() {
        Tag tag;
        Mat slaveL = null;
        // compute the first eigenvector iterately
        int remain = slaveNum;
        svd.setL(Like);
        String output = dispArray(Like.data);   // information need to show
        // send L
        for (int i = 1; i <= slaveNum; i++){
            sendMat(Like,i,commu);
        }
        //send Tag
        ArrayList<Tag> index = split.split();
        for(int i = 0; i < index.size(); i++) {
            tag = index.get(i);
            CommonPacket packet = new CommonPacket(-1,tag);
            commu.sendPacket(i+1, packet);
        }
        // receive L and update
        while (remain > 0) {
            synchronized (mList) {
                if (mList.size() > 0) {
                    slaveL = getMat(mList);
                    svd.update_SVD(slaveL);
                    remain--;
                }
            }
        }

        this.Like = svd.getUpdateL();
        MatOp.vectorNormalize(this.Like, MatOp.NormType.NORM_L2);
        return output;
    }

    public boolean isCompleted() {
        return svd.isPerformed(Like);
        //return svd.isPerformed();
    }
    public String dispFinal() {
        String finalout = "final  " + dispArray(this.Like.data);   // final information
        return finalout;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // 4 slaves assumed
        //double[] test = new double[1000 * 1000];
        int rows = KSVDconstant.rows;
        int cols = KSVDconstant.cols;
        double[] test = new double[rows * cols];
        int q = 0;
        int slaveNum = 2;
        LinkedList<Double[]> mList = new LinkedList<Double[]>();

        //String dir = "tachyon://localhost:19998";
        //String fileName = "/BinData";
        String dir = "./resource";
        String fileName = "/BinData";
        int port = Integer.parseInt(args[0]);
        MasterMiddleWare commu = new MasterMiddleWare(port);
        DistributedSVD svd = new DistributedSVD(commu, slaveNum, test);
        commu.startMaster();
        int K = 3;
        while(K > 0) {

            try {
                FileSystemInitializer fs = FileSystemAdaptorFactory.BuildFileSystemAdaptor(FileSystemType.LOCAL, dir);
                DataHandler t = DataHandlerFactory.BuildDataHandler(FileSystemType.LOCAL);
//            test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
//            System.out.println(test[1000 * 1000 - 1]);
                test = t.getDataInDouble(fs.getFsHandler(), fileName, rows * cols);
                System.out.println(test[rows * cols - 1]);
            } catch (IOException e) {
            }



            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                String line = br.readLine();
                if (line.startsWith("show")) {
                    System.out.println("Current Connected Slave : " + commu.slaveNum());
                } else if (line.startsWith("start")) {
                    Thread t = new Thread(svd);
                    t.start();
                } else {
                    break;
                }

            }
            K--;

        }

    }

    public static void printArray(double[] arr) {
        for (double i : arr)
            System.out.print(i + " ");
        System.out.println();
    }

    private String dispArray(double[] arr){
        String s = "";
        for(double i: arr)
            s+= i + " ";
        s+= "\n";
        return s;
    }


    public static Mat getMat(LinkedList<Double[]> mList) {
        Double[] temp = mList.peek();
        double row = temp[0];
        double col = temp[1];
        double[] arr = new double[temp.length - 2];
        for (int k = 0; k < arr.length; k++) {
            arr[k] = temp[k + 2];
        }
        Mat mat = new Mat((int) row, (int) col, arr);
        mList.remove();
        return mat;
    }

    public static void sendMat(Mat mat, int id, MasterMiddleWare m) {
        Double[] array = new Double[mat.data.length + 2];
        array[0] = Double.valueOf(mat.rows);
        array[1] = Double.valueOf(mat.cols);

        for (int k = 0; k < mat.data.length; k++)
            array[k + 2] = Double.valueOf(mat.data[k]);
        CommonPacket packet = new CommonPacket(-1, array);

        m.sendPacket(id, packet);
    }
}
