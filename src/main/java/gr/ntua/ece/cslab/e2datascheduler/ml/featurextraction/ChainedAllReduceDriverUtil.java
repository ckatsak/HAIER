package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction;


import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.reduce.TornadoReduce;
//import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.AccelerationData;

import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.DelegatingConfiguration;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.flink.FlinkData;

import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.transformReduceUDF;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.ExamineUDF.inOwner;


/**
 * TODO(ckatsak): Documentation
 */
public class ChainedAllReduceDriverUtil {


    private final ReduceFunction reducer;
    private final byte[] inputByteData;
    private final int numOfElements;

    public ChainedAllReduceDriverUtil(final ReduceFunction reduceFunction) {
        this.reducer = reduceFunction;
        this.inputByteData = new byte[42];
        this.numOfElements = 42;
    }


    // --------------------------------------------------------------------------------------------


    public void fakeCompile() {
        String reduceUserClassName = this.reducer.getClass().toString()
                .replace("class ", "").replace(".", "/");
        TornadoReduce rsk = transformReduceUDF(reduceUserClassName);
//        Configuration conf = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig();
//        AccelerationData acdata = (AccelerationData) conf.getAccelerationData();
//        inputByteData = acdata.getRawData();
//        numOfElements = acdata.getInputSize();
        switch (inOwner) {
            case "java/lang/Integer": {
                int[] in = new int[numOfElements];

                // Based on the output data type create the output array and the Task Schedule
                // Currently, the input and output data can only be of type Integer, Double, Float or Long.
                // Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
                // since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.

                int[] out = new int[1];
                byte[] outBytes = new byte[4];
                FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

                new TaskSchedule("s0")
                        .flinkInfo(f)
                        .task("t0", rsk::reduce, in, out)
                        .streamOut(out)
                        .execute();

//                AccelerationData acres = new AccelerationData(outBytes, 1, 4);
//                conf.setAccelerationData(acres);
                break;
            }
            case "java/lang/Double": {
                double[] in = new double[numOfElements];
                double[] out = new double[1];
                byte[] outBytes = new byte[8];
                FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

                new TaskSchedule("s0")
                        .flinkInfo(f)
                        .task("t0", rsk::reduce, in, out)
                        .streamOut(out)
                        .execute();

//                AccelerationData acres = new AccelerationData(outBytes, 1, 8);
//                conf.setAccelerationData(acres);
                break;
            }
            case "java/lang/Float": {
                float[] in = new float[numOfElements];

                float[] out = new float[1];
                byte[] outBytes = new byte[4];
                FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

                new TaskSchedule("s0")
                        .flinkInfo(f)
                        .task("t0", rsk::reduce, in, out)
                        .streamOut(out)
                        .execute();

//                AccelerationData acres = new AccelerationData(outBytes, 1, 4);
//                conf.setAccelerationData(acres);
                break;
            }
            case "java/lang/Long": {
                long[] in = new long[numOfElements];
                long[] out = new long[1];
                byte[] outBytes = new byte[8];
                FlinkData f = new FlinkData(rsk, true, inputByteData, outBytes);

                new TaskSchedule("s0")
                        .flinkInfo(f)
                        .task("t0", rsk::reduce, in, out)
                        .streamOut(out)
                        .execute();

//                AccelerationData acres = new AccelerationData(outBytes, 1, 8);
//                conf.setAccelerationData(acres);
                break;
            }
        }
//        this.outputCollector.collect(null);
    }

}
