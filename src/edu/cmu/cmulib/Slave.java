package edu.cmu.cmulib;

import edu.cmu.cmulib.CoolMatrixUtility.core.Mat;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Slave_SVD;
import edu.cmu.cmulib.CoolMatrixUtility.decomp.svd.Slave_getSplitedMatrix;
import edu.cmu.cmulib.CoolMatrixUtility.help.Tag;

import java.io.IOException;
import java.util.LinkedList;

import edu.cmu.cmulib.FileSystemAdaptor.*;
import edu.cmu.cmulib.Communication.CommonPacket;
import edu.cmu.cmulib.Utils.ConfParameter;

public class Slave {
	public int SlaveId;
	public double workspan = Double.MAX_VALUE;
    String address = "localhost";
    int port = 8888;
    String dir = "./resource";
    String fileName = "/BinData";

    LinkedList<Double[]> mList = null;
    LinkedList<Tag> tagList = null;
    Mat score = null;
    Mat S = null;
    Mat L = null;
    SlaveMiddleWare sdSlave = null;
    Slave_getSplitedMatrix split = null;
    Slave_SVD svd = null;

    public Slave(ConfParameter conf) {
        this.address = conf.masterAddress;
        this.port = conf.masterPort;
        this.dir = conf.fileDir;
        this.fileName = conf.fileName;
    }
	
	public static void printArray(double[] arr){
		for(double i: arr)
			System.out.print(i+" ");
		System.out.println();
	}

    public void init() throws IOException {
        double[] test = new double[1000*1000];
        int rows = 1000;
        int cols = 1000;

        try {
            FileSystemInitializer fs = FileSystemAdaptorFactory.BuildFileSystemAdaptor(FileSystemType.LOCAL, dir);
            DataHandler t = DataHandlerFactory.BuildDataHandler(FileSystemType.LOCAL);
            test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
            System.out.println(test[1000*1000-1]);
        } catch (IOException e) {
        }

        mList = new LinkedList<Double[]>();
        tagList = new LinkedList<Tag>();

        score = new Mat(rows, cols ,test);

        sdSlave = new SlaveMiddleWare(address, port);
        sdSlave.register(Double[].class, mList);
        sdSlave.register(Tag.class, tagList);
        System.out.println(address + " " + port);
        sdSlave.startSlave();

        split = new Slave_getSplitedMatrix(score);
        svd = new Slave_SVD();
    }

    public void execute() {
        while(true){
            //receive tag and compute L
            synchronized (tagList) {
                if (tagList.size() > 0) {
                    split.setTag(tagList.peek());
                    tagList.remove();
                    S = split.construct();
                    L = svd.Slave_UpdateL(S);
                    printArray(L.data);
                    sendMat(L,sdSlave);

                }
            }
            //receive L
            synchronized (mList) {
                if (mList.size() > 0) {
                    System.out.println("enter slave synchronized");
                    L = getMat(mList);
                    svd.setL(L);
                }
            }
        }
    }
	/*
	public static void main (String[] args) throws IOException {
        
        // initialize original matrix
        double[] test = new double[1000*1000];
		int rows = 1000;
		int cols = 1000;
        String address = args[0];
        int port = Integer.parseInt(args[1]);
        int q = 0;

        //String dir = "tachyon://localhost:19998";
        //String fileName = "/BinData";
        String dir = "./resource";
        String fileName = "/BinData";

        try {
            FileSystemInitializer fs = FileSystemAdaptorFactory.BuildFileSystemAdaptor(FileSystemType.LOCAL, dir);
            DataHandler t = DataHandlerFactory.BuildDataHandler(FileSystemType.LOCAL);
            test = t.getDataInDouble(fs.getFsHandler(), fileName, 1000 * 1000);
            System.out.println(test[1000*1000-1]);
        } catch (IOException e) {
        }
    
		LinkedList<Double[]> mList = new LinkedList<Double[]>();
        LinkedList<Tag> tagList = new LinkedList<Tag>();
        
		Mat score = new Mat(rows, cols ,test);
        Mat S, L;

        //String address = InetAddress.getLocalHost().getHostAddress();
        SlaveMiddleWare sdSlave = new SlaveMiddleWare(address, port);
        sdSlave.register(Double[].class, mList);
        sdSlave.register(Tag.class, tagList);
        System.out.println(address + " " + port);
        sdSlave.startSlave();
        
		Slave_getSplitedMatrix split = new Slave_getSplitedMatrix(score);
		Slave_SVD svd = new Slave_SVD();

        // update L using equation L=SS(trans)L
        while(true){
            //receive tag and compute L
            synchronized (tagList) {
                if (tagList.size() > 0) {
                    split.setTag(tagList.peek());
                    tagList.remove();
                    S = split.construct();
                    L = svd.Slave_UpdateL(S);
                    printArray(L.data);
                    sendMat(L,sdSlave);

                }
            }
            //receive L
            synchronized (mList) {
                if (mList.size() > 0) {
                    System.out.println("enter slave synchronized");
                    L = getMat(mList);
                    svd.setL(L);
                }
            }
        }
	}
	*/
	
	public static Mat getMat(LinkedList<Double[]> mList){
		Double [] temp = mList.peek();
    	double row = temp[0];
    	double col = temp[1];
    	double [] arr = new double[temp.length-2];
    	for(int k=0;k<arr.length;k++){
    		arr[k] = temp[k+2];
    	}
    	Mat mat = new Mat((int)row,(int)col,arr);    	
        mList.remove();
        return mat;
		
	}
	
	public static void sendMat(Mat mat,SlaveMiddleWare m){
		Double [] array = new Double[mat.data.length+2];
	    array[0] = Double.valueOf(mat.rows);
	    array[1] = Double.valueOf(mat.cols);
	    
	    for(int k=0; k<mat.data.length;k++)
	    	array[k+2] = Double.valueOf(mat.data[k]);
        CommonPacket packet = new CommonPacket(-1, array);
        
        m.sendPacket(packet);
	}
}
