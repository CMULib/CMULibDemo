package edu.cmu.cmulib;//import cmu.core.Mat;
import edu.cmu.cmulib.Communication.*;

import java.util.LinkedList;
import java.util.Queue;
import java.io.IOException;

public class MasterMiddleWare implements MiddleWare {
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
    //
    private MasterNode masterNode;

    public Queue<CommonPacket> packets;
    
    public PacketHandler packetHandler;

    public int port;
    
    public void register(Class<?> clazz, Queue list){
		packetHandler.register(clazz, list);
	}
    
    public MasterMiddleWare(int nPort) {
        port = nPort;
    	packets = new LinkedList<CommonPacket>();
    	packetHandler = new PacketHandler();
    }

    public void startMaster() {
        try {
            masterNode = new MasterNode(port, this);
            masterNode.startListen();
        }catch (IOException e){
            System.out.println("Failed to start Master");
        }
    }

    public void sendPacket(int id, CommonPacket packet){
        masterNode.sendObject(id, packet);
    }

    public int slaveNum(){
        if(masterNode != null)
            return masterNode.slaveNum();
        else
            return 0;
    }

    public void msgReceived(int nodeId, CommonPacket packet) {
    	packet.setSlaveId(nodeId);

    	synchronized(packetHandler){
      
    	    packetHandler.handlePacket(packet.getObject());
    	}
    }
}
