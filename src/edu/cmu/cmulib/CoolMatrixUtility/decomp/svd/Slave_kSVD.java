package edu.cmu.cmulib.CoolMatrixUtility.decomp.svd;

/**
 * Created by chouclee on 4/22/15.
 */
import edu.cmu.cmulib.CoolMatrixUtility.core.Mat;
import edu.cmu.cmulib.CoolMatrixUtility.core.MatOp;

public class Slave_kSVD {
    private Mat L;//src;          // original L and source Matrix
    private int k;

    public Slave_kSVD(Mat src){
        // 	this.src = src;
        this.L = null;
    }
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
        Mat tempL;
        for (int j = 0; j < k; j++) {
            Mat Lj = L.colRange(j, j + 1);
            this.L.setCols(j, j + 1, MatOp.gemm(src, MatOp.gemm(src.t(), Lj)));
            src = MatOp.scaleAdd(MatOp.gemm(Lj, MatOp.gemm(src.t(), Lj).t()), -1, src);
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

