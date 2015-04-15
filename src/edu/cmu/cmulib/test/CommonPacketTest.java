package edu.cmu.cmulib.test;

import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.cmu.cmulib.Communication.CommonPacket;

/**
 *
 * Write detailed test cases for MasterNode and SlaveNode classes.
 * @author Jing Li
 *
 */
public class CommonPacketTest {

    /**
     *  parameters used for test
     */

    //private static final long serialVersionUID = 4L;
    private int slaveId;
    private int slaveId2;
    private String classType;
    private Object ob;
    private CommonPacket cPacket;
    private CommonPacket cPacket2;

    /**
     * Called before each test.
     */
    @Before
    public void setUp(){
        cPacket = new CommonPacket(slaveId, classType, ob);
        cPacket2 = new CommonPacket(slaveId, ob);
    }

    @Test
    public void testRunning(){
        assertNotNull(cPacket.getSlaveId());
        assertNotNull(cPacket2.getObject());
        cPacket.setSlaveId(slaveId2);
    }

    @After
    public void cleanUp(){

    }
}