package edu.cmu.cmulib;//import cmu.core.Mat;
import edu.cmu.cmulib.Communication.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SlaveMiddleWare implements MiddleWare {
    //
    private SlaveNode slaveNode;

    public ConcurrentLinkedDeque<CommonPacket> packets;
    
    public PacketHandler packetHandler;

    public String address;
    public int port;

    public class MsgItem {
        public int opId;
        //public Mat data;
        public double para;
        public int fromId;

        public MsgItem(int nId, int nOpId, double nPara) {
            opId = nOpId;
            para = nPara;
            fromId = nId;
        }
    }

    public SlaveMiddleWare(String nAddress, int nPort) {
    	packets = new ConcurrentLinkedDeque<>();
    	packetHandler = new PacketHandler();
        address = nAddress;
        port = nPort;
    }

    public void startSlave() throws IOException{
        slaveNode = new SlaveNode(address, port, this);
        slaveNode.connect();
    }
    
    public void register(Class<?> clazz, ConcurrentLinkedDeque queue){
		packetHandler.register(clazz, queue);
	}

    public void sendPacket(CommonPacket packet){
    	slaveNode.send(packet);
    }

    public void msgReceived(int nodeID, CommonPacket packet) {
    	//synchronized (packets) {
            packets.add(packet);
        //}
    	
    	//synchronized(packetHandler){
    	    packetHandler.handlePacket(packet.getObject());
    	//}

        System.out.println("on received");
    }
}
