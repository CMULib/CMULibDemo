package edu.cmu.cmulib.test;

import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.cmu.cmulib.Communication.Message;


/**
 *
 * Write detailed test cases for Message classes.
 * @author Jing Li
 *
 */
public class MessageTest {

    /**
     *  parameters used for test
     */
    //private int opCode;
    private String message;
    private int matrixInteger[][];
    private double matrixDouble;
    private double matrixDouble2[][];
    private int matrixIntegerM;
    private int matrixIntegerN;
    private Message msg;
    //private int m;
    //private int n;

    /**
     * Called before each test.
     */
    @Before
    public void setUp(){
        msg = new Message(message);
    }

    /**
     * Test the functional methods
     * @throws Exception
     * 			the exception to throw
     */
    @SuppressWarnings("static-access")
    @Test
    public void testRunningBlocks() throws Exception{
        assertNotNull(msg.buildParameter(matrixDouble));
        assertNotNull(msg.buildMatrix(matrixInteger, matrixIntegerM, matrixIntegerN));
        assertNotNull(msg.buildMatrixDouble(matrixDouble2, matrixIntegerM, matrixIntegerN));
        msg.extractMessage(message);
    }

    @After
    public void cleanUp(){

    }

}