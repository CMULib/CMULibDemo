package edu.cmu.cmulib.CoolMatrixUtility.decomp.svd;

/**
 * Created by chouclee on 4/22/15.
 */
import edu.cmu.cmulib.CoolMatrixUtility.core.Mat;
import edu.cmu.cmulib.CoolMatrixUtility.core.MatOp;

public class Slave_kSVD {
    private Mat L;//src;          // original L and source Matrix
    private int k;

    public Slave_kSVD(int k){
        this.k = k;
    }

    public Slave_kSVD() {
        this.k = 1;
    }
    /**
     * Slave_UpdateL
     *
     * update L by using the formula L=SS(transpose)L
     */
    public Mat Slave_UpdateL(Mat src) {
        //Mat tempSrc = src.clone();
        Mat tempSrc = src.colRange(0, src.cols - 1);
        for (int j = 0; j < k; j++) {
            //System.out.printf("Calculating %dth column\n", j+1);
            Mat Lj = L.colRange(j, j);
            this.L.setCols(j, j, MatOp.gemm(tempSrc, MatOp.gemm(tempSrc.t(), Lj)));
            tempSrc = MatOp.diff(tempSrc, MatOp.gemm(Lj, MatOp.gemm(tempSrc.t(), Lj).t()));
        }
        return this.L;
    }

    /**
     * setL
     *
     * set L after receiving from master
     */
    public void setL(Mat L){
        this.L = L;
    }
    /**
     * setS
     *
     * set matrix after reconstructing based on tag
     */
    public void setS(Mat S){
        // this.src = S;
    }
}

