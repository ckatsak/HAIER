package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map;

import org.apache.flink.api.java.tuple.Tuple3;

import uk.ac.manchester.tornado.api.annotations.Parallel;


/**
 * The functions that will be executed on Tornado.
 */
public class TornadoMap4 {

    public MiddleMap4 mdm;

    public TornadoMap4(MiddleMap4 mdm) {
        this.mdm = mdm;
    }

    public void mapCp(Tuple3[] in, Tuple3[] out) {

        for (@Parallel int i = 0; i < in.length; i++) {
            out[i] = mdm.mymaptuple3tuple3(in[i]);
        }

    }

}