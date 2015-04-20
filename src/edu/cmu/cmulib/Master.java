package edu.cmu.cmulib;

import java.util.ArrayList;
import java.util.LinkedList;

import edu.cmu.cmulib.CoolMatrixUtility.core.Mat;
import edu.cmu.cmulib.CoolMatrixUtility.core.MatOp;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Master_SVD;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Master_Spliter;
import edu.cmu.cmulib.CoolMatrixUtility.help.Tag;
import edu.cmu.cmulib.Utils.ConfParameter;

import java.io.IOException;

import edu.cmu.cmulib.FileSystemAdaptor.*;

import edu.cmu.cmulib.Communication.CommonPacket;
import edu.cmu.cmulib.Utils.JsonParser;
import org.json.simple.parser.ParseException;

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
        this.slaveNum = 1;
        this.dir = "";
        this.fileName = "";
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
        double[] test = new double[1000*1000];
        mList = new LinkedList<Double[]>();
        try {
            FileSystemInitializer fs = FileSystemAdaptorFactory.BuildFileSystemAdaptor(mFsType, dir);
            DataHandler t = DataHandlerFactory.BuildDataHandler(mFsType);
            test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
            System.out.println(test[1000 * 1000 - 1]);
        } catch (IOException e) {
        }

        // initialize original matrix
        int rows = 1000;
        int cols = 1000;
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
/*
    public static void main(String[] args) throws IOException, InterruptedException {
        // 4 slaves assumed
        double[] test = new double[1000 * 1000];
        int q = 0;
        int slaveNum = 1;
        LinkedList<Double[]> mList = new LinkedList<Double[]>();

        //String dir = "tachyon://localhost:19998";
        //String fileName = "/BinData";
        String dir = "./resource";
        String fileName = "/BinData";
        try {
            FileSystemInitializer fs = FileSystemAdaptorFactory.BuildFileSystemAdaptor(FileSystemType.LOCAL, dir);
            DataHandler t = DataHandlerFactory.BuildDataHandler(FileSystemType.LOCAL);
            test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
            System.out.println(test[1000 * 1000 - 1]);
        } catch (IOException e) {
        }

        int rows = 1000;
        int cols = 1000;
        Mat score = new Mat(rows, cols, test);
        Tag tag;
        Mat Like, slaveL;

        int port = Integer.parseInt(args[0]);

        MasterMiddleWare commu = new MasterMiddleWare(port);
        commu.register(Double[].class, mList);
        commu.startMaster();


        Master_Spliter split = new Master_Spliter(score, slaveNum);
        Master_SVD svd = new Master_SVD(score, slaveNum);
        while (commu.slaveNum() < slaveNum) {
            System.out.println(commu.slaveNum());
        }
        Like = svd.initL();
        slaveL = null;

        // compute the first eigenvector iterately
        do {
            int remain = slaveNum;
            svd.setL(Like);
            printArray(Like.data);
            // send L
            for (int i = 1; i <= slaveNum; i++) {
                sendMat(Like, i, commu);
            }
            //send Tag
            ArrayList<Tag> index = split.split();
            for (int i = 0; i < index.size(); i++) {
                tag = index.get(i);
                CommonPacket packet = new CommonPacket(-1, tag);
                commu.sendPacket(i + 1, packet);
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

            Like = svd.getUpdateL();
            MatOp.vectorNormalize(Like, MatOp.NormType.NORM_L2);
        } while (!svd.isPerformed(Like));     //termination of iteration
        System.out.println("final  ");
        printArray(Like.data);
    }
*/
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
