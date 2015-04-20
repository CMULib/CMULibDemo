package edu.cmu.cmulib.Communication;

import javax.security.auth.callback.Callback;

import java.net.*;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;

public class MasterNode {
    HashMap<Integer, edu.cmu.cmulib.Communication.SlaveData> slaveMap;
    int slaveId=1;
    private int port = -1;
    private ExecutorService executorService;
    private ServerSocket serverSocket;
    private final int POOL_SIZE = 5;
    public edu.cmu.cmulib.Communication.MiddleWare midd;

    //private SDMiddleWare middleWare;
    private Callback middleWare;
    // contructor 
    public MasterNode(int port, edu.cmu.cmulib.Communication.MiddleWare nmidd) throws IOException {
        System.out.println("I'm a MasterNode!");
        slaveMap = new HashMap<Integer, SlaveData>();
        serverSocket = new ServerSocket(port);
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * POOL_SIZE);
        this.port = port;
        midd = nmidd;
    }

    public void startListen () throws IOException {
        new Thread(new ServerService()).start();
    }

    public void sendObject(int id, CommonPacket packet){
        SlaveData aSlaveData = slaveMap.get(id);
        try {
			aSlaveData.oos.writeObject(packet);
		} catch (IOException e1) {
			System.out.println("cannot write packet");
			e1.printStackTrace();
		}
        
        try {
			aSlaveData.oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public int slaveNum(){
        return slaveMap.size();
    }

    private class ServerService implements Runnable{
        public void run(){
            while(true){
                Socket socket = null;
                try {
                    socket = serverSocket.accept();
                    System.out.println("socket accepted");
                    executorService.execute(new Slave(socket));
                }catch (Exception e) {
                    System.out.println(e);
                }
            }
        }
    }

    private class Slave implements Runnable {
        private Socket socket;
        private ObjectOutputStream oos;
        private ObjectInputStream ois;

        public Slave(Socket socket){
            this.socket = socket;
            try {
				oos = new ObjectOutputStream(socket.getOutputStream());
			} catch (IOException e) {
				System.out.println("fail to get object ouput stream");
				e.printStackTrace();
			}
            try {            
				ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
			} catch (IOException e) {
				System.out.println("fail to get object input stream");
				e.printStackTrace();
			}
            System.out.println("socket connected");
        }

        public void run() {
            try {
                handleSocket();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void handleSocket() throws IOException {
            String temp="";
            SlaveData aSlave = new SlaveData(ois, oos);
            aSlave.id = slaveId++;
            synchronized(slaveMap){
                slaveMap.put(aSlave.id,aSlave);
            }

            while(true){
                CommonPacket packet = null;
                try {
                    packet = (CommonPacket) ois.readObject();
                } catch (IOException e) {
                    break;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                midd.msgReceived(aSlave.id,packet);
                if(temp.equals("eof")){
                    System.out.println("it is eof");
                    break;
                }
            }
            oos.close();
            ois.close();
            socket.close();
            synchronized(slaveMap){
                slaveMap.remove(aSlave.id);
            }
        }

    }
}
