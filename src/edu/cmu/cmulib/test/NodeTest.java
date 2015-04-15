package edu.cmu.cmulib.test;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.cmu.cmulib.Communication.CommonPacket;
import edu.cmu.cmulib.Communication.MasterNode;
import edu.cmu.cmulib.Communication.MiddleWare;
import edu.cmu.cmulib.Communication.SlaveNode;


/**
 * Write detailed test cases for MasterNode and SlaveNode classes.
 * @author Jing Li
 */
public class NodeTest {

    private MasterNode mNode;
    private SlaveNode sNode;
    private SlaveNode sNode2;
    private MiddleWare mid;
    private String slaveName;
    //private Socket socket;
    private int id;
    private CommonPacket packet;
    private String masterAddress;
    private int masterPort;
    //private MiddleWare myMidd;

    /**
     * Called before each test.
     * @throws IOException
     * 			the exception to throw
     */
    @Before
    public void setUp() throws IOException {
        // comment/uncomment these lines to test each class
        //mNode = new MasterNode(mid);
        //sNode = new SlaveNode(slaveName);
        sNode2 = new SlaveNode(masterAddress, masterPort, mid);
    }
    @Test
    public void testMasterRunning() throws IOException{
        mNode.startListen();
        mNode.sendObject(id, packet);
        assertNotNull(mNode.slaveNum());
    }
    @Test
    public void testSlaveRunning(){
        sNode.connect();
        sNode.disconnect();
        sNode.send(packet);
        sNode2.connect();
        sNode2.disconnect();
        sNode2.send(packet);
    }

    @After
    public void cleanUp(){

    }


}
