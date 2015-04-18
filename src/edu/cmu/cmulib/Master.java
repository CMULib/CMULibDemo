package edu.cmu.cmulib;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;

import edu.cmu.cmulib.CoolMatrixUtility.core.Mat;
import edu.cmu.cmulib.CoolMatrixUtility.core.MatOp;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Master_SVD;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Master_Spliter;
import edu.cmu.cmulib.CoolMatrixUtility.help.Tag;

import java.io.IOException;

import edu.cmu.cmulib.FileSystemAdaptor.*;

import edu.cmu.cmulib.Communication.CommonPacket;

import static java.lang.Thread.sleep;

/*
   How to use this
   Master master = new Master(4, "/BinData", 8888);
   master.init();
   do {
     String str = master.execute();
   while(!master.isCompleted());
    String final = master.dispFinal();
  */
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

    public Master() {
        this.slaveNum = 4;
        this.dir = "./resource";
        this.fileName = "/BinData";
        this.port = 8888;
    }
    public Master(int slaveNum, String dir, String fileName, int port) {
        this.slaveNum = slaveNum;
        this.dir = dir;
        this.fileName = fileName;
        this.port = port;
    }
    public void init() throws IOException {
        double[] test = new double[1000*1000];
        mList = new LinkedList<Double[]>();
        try {
            FileSystemInitializer fs = new TachyonInitialier();
            fs.connect(dir);
            DataHandler t = new TachyonDataHandler();
            test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
            System.out.println(test[1000 * 1000 - 1]);
            //sleep(10000);
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

    public static void main(String[] args) throws IOException, InterruptedException {
        // 4 slaves assumed
        double[] test = new double[1000 * 1000];
        int q = 0;
        int slaveNum = 4;
        LinkedList<Double[]> mList = new LinkedList<Double[]>();


        /*
        Read BinData
         */
        //String dir = "tachyon://localhost:19998";
        //String fileName = "/BinData";
        String dir = "./resource";
        String fileName = "/BinData";
        try {
            FileSystemInitializer fs = new LocalFsInitializer();
            fs.connect(dir);
            DataHandler t = new LocalDataHandler();
            test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
            System.out.println(test[1000 * 1000 - 1]);
            //sleep(10000);
        } catch (IOException e) {
        }
        /*
        Read normal data
         */
//        BufferedReader br = new BufferedReader(new FileReader("./resource/svd.data.txt"));
//        String line;
//        while ((line = br.readLine()) != null) {
//            test[q] = Double.parseDouble(line);
//            q++;
//        }
//        br.close();


        // start service of master node

        int port = Integer.parseInt(args[0]);

        MasterMiddleWare commu = new MasterMiddleWare(port);

        DistributedSVD svd = new DistributedSVD(commu,slaveNum,test);

        commu.startMaster();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));


        while (true) {
            String line = br.readLine();
            if(line.startsWith("show")){
                System.out.println("Current Connected Slave : "+ commu.slaveNum());
            }else if(line.startsWith("start")){
                Thread t = new Thread(svd);
                t.start();
            }
        }


    }
        /*
        System.out.println("PPPPPPPPPPPPPPPP");
        double [] a = {1.1, 2.2, 3.3, 4.4};
        int count =0;
        while (count<10){
        	count++;
        	int remain = 4;
            while (commu.slaveNum() != slaveNum){System.out.println(commu.slaveNum());}
            for (int i = 1; i <= slaveNum; i++){
            	Mat mat = new Mat(2,2,a);
        	    sendMat(mat,i,commu);
                
            }
            
            while (remain > 0) {
            	
                synchronized (mList) {
                    if (mList.size() > 0) {                
                    	Mat mat = getMat(mList);
                    	a = mat.data;
                    	remain--;
                    	
                    }
               }
            }
            System.out.println(a[0]+" "+a[1]+" "+a[2]+" "+a[3]);
        }
        */
        /*
        Master_Spliter split = new Master_Spliter(score, slaveNum);
		Master_SVD svd = new Master_SVD(score, slaveNum);
		
		Like = svd.initL();
		slaveL = null;
		do {
			svd.setL(Like);
			commu.push(Like);
			ArrayList<Tag> index = split.split();
			for(int i = 0; i < index.size(); i++) {
				tag = index.get(i);
				commu.push(tag);
			}
			for (int i = 0; i < slaveNum; i++) {
//				do {
					slaveL = commu.pull();
//				} while (slaveL == null);
				svd.update_SVD(slaveL);
			}
			Like = svd.getUpdateL();
			MatOp.vectorNormalize(Like, MatOp.NormType.NORM_L2);
//			System.out.println(Like.data[0] + "  " + Like.data[1]+ "  " + Like.data[2]);
		} while (!svd.isPerformed(Like));		
		System.out.println("final  " + Like.data[0] + "  " + Like.data[1]+ "  " + Like.data[2]);
		*/
        /*Double[] a = {1.1, 2.2, 3.3, 4.4};

        while (a[0] + a[1] + a[2] + a[3] < 100.0) {
            int remain = 4;
            while (commu.slaveNum() != slaveNum) {System.out.print(commu.slaveNum());}
            System.out.println("\n");

            for (int i = 1; i <= slaveNum; i++) {
            	CommonPacket packet = new CommonPacket(-1,a[i - 1]);
            	System.out.println("before send packet");
                commu.sendPacket(i, packet);
                System.out.println("after send packet");
            }

            while (remain > 0) {
                synchronized (commu.msgs) {
                    if (commu.msgs.size() > 0) {
                        System.out.println(commu.msgs.peek().para);
                        a[commu.msgs.peek().fromId - 1] = commu.msgs.peek().para;
                        commu.msgs.remove();
                        remain--;
                    }
                }
            }

            double sum = a[0] + a[1] + a[2] + a[3];*/
        //System.out.println("sum :" + sum);

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
