package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction;

//import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.AccelerationData;

import org.apache.flink.configuration.Configuration;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.common.Access;
import uk.ac.manchester.tornado.api.common.TornadoDevice;
import uk.ac.manchester.tornado.api.flink.FlinkData;
import uk.ac.manchester.tornado.api.runtime.TornadoRuntime;

//import java.util.ArrayList;
//import java.util.HashMap;
import java.util.ResourceBundle;

//import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.doubleToByte;
//import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.longToByte;


/**
 * TODO(ckatsak): Documentation
 */
public class ReduceDriverUtil {

    private static final ResourceBundle resourceBundle = ResourceBundle.getBundle("config");
    private static final String preCompiledBasePath = resourceBundle.getString("tornado.precompiled.path");


    // --------------------------------------------------------------------------------------------


    public void fakeCompile() {
        final TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();

        int numberOfGroupElements = 42;
        int size = 2;

        int[] ini = new int[numberOfGroupElements];
        double[] ind = new double[numberOfGroupElements];
        double[] ind2 = new double[numberOfGroupElements];
        long[] inl = new long[numberOfGroupElements];

        int[] outi = new int[(size + 1)];
        double[] outd = new double[(size + 1)];
        double[] outd2 = new double[(size + 1)];
        long[] outl = new long[(size + 1)];

        byte[] virtualBytes = new byte[42];
        FlinkData f3 = new FlinkData(true, virtualBytes, virtualBytes, virtualBytes, virtualBytes);

        new TaskSchedule("s3")
                .flinkInfo(f3)
                .prebuiltTask(
                        "t3",
                        "reduce",
                        preCompiledBasePath + "prebuilt-reduceFlink.cl",
                        new Object[]{
                                ini, ind, ind2, inl,
                                outi, outd, outd2, outl
                        },
                        new Access[]{
                                Access.READ, Access.READ, Access.READ, Access.READ,
                                Access.WRITE, Access.WRITE, Access.WRITE, Access.WRITE
                        },
                        defaultDevice,
                        new int[]{numberOfGroupElements}
                )
                .streamOut(outi, outd, outd2, outl)
                .execute();
    }


    public void fakeCompile(final Configuration conf) {
////        Configuration conf = this.taskContext.getTaskConfig().getConfiguration();
////        Configuration predConf = conf.getPredecessorConfig(0);
////        AccelerationData acdata = (AccelerationData) predConf.getAccelerationData();
//        AccelerationData acdata = new AccelerationData();
//        int[] numberOfElementsPerGroup = acdata.getGroupByData().getNumberOfElementsPerGroup();
//        HashMap<Integer, ArrayList<byte[]>> bytesOfGroup = acdata.getGroupByData().getBytesOfGroup();
//        int returnSize = acdata.getReturnSize();
//        int numOfGroups = numberOfElementsPerGroup.length;
        int numOfGroups = 1;
//        byte[] reduceGroupByRes = new byte[numOfGroups * returnSize];
//        int bytesWritten = 0;

        for (int i = 0; i < numOfGroups; i++) {
            //long redInitStart = System.currentTimeMillis();
//            int numberOfGroupElements = numberOfElementsPerGroup[i];
            int numberOfGroupElements = 42;
//            byte[] idBytes =  bytesOfGroup.get(i).get(0);
//            byte[] pointxBytes =  bytesOfGroup.get(i).get(1);
//            byte[] pointyBytes =  bytesOfGroup.get(i).get(2);
//            byte[] counterBytes = bytesOfGroup.get(i).get(3);
            byte[] fakeCompilationBytes = new byte[42];

//            int size;
            int size = 2;
//            if (numberOfGroupElements <= 256) {
//                size = 2;
//            } else {
//                size = numberOfGroupElements / 256;
//            }

            // ---------------
            int[] ini = new int[numberOfGroupElements];
            double[] ind = new double[numberOfGroupElements];
            double[] ind2 = new double[numberOfGroupElements];
            long[] inl = new long[numberOfGroupElements];

            int[] outi = new int[(size + 1)];
            double[] outd = new double[(size + 1)];
            double[] outd2 = new double[(size + 1)];
            long[] outl = new long[(size + 1)];

//            FlinkData f3 = new FlinkData(true, idBytes, pointxBytes, pointyBytes, counterBytes);
            FlinkData f3 = new FlinkData(true, fakeCompilationBytes, fakeCompilationBytes, fakeCompilationBytes, fakeCompilationBytes);

            // task schedule for centroid accumulation
            TornadoDevice defaultDevice = TornadoRuntime.getTornadoRuntime().getDefaultDevice();
            //			long redInitEnd = System.currentTimeMillis();
            //System.out.println("### Initialize arrays for reduction " + i + ": " + (redInitEnd - redInitStart));

            new TaskSchedule("s3")
                    .flinkInfo(f3)//
                    .prebuiltTask("t3", //
                            "reduce", //
                            preCompiledBasePath + "prebuilt-reduceFlink.cl", //
                            new Object[]{ini, ind, ind2, inl, outi, outd, outd2, outl}, //
                            new Access[]{Access.READ, Access.READ, Access.READ, Access.READ, Access.WRITE, Access.WRITE, Access.WRITE, Access.WRITE}, //
                            defaultDevice, //
                            new int[]{numberOfGroupElements})
                    //
                    .streamOut(outi, outd, outd2, outl) //
                    .execute();

//            long red = System.currentTimeMillis();
            //System.out.println("### Execute Reduction " + i + ": " + (red - redInitEnd));

//            for (int j = 1; j < outd.length; j++) {
//                outd[0] += outd[j];
//            }
            //System.out.println("**** Point x: " + outd[0]);
//            for (int j = 1; j < outd2.length; j++) {
//                outd2[0] += outd2[j];
//            }
            //System.out.println("**** Point y: " + outd2[0]);
//            for (int j = 1; j < outl.length; j++) {
//                outl[0] += outl[j];
//            }
            //System.out.println("**** Counter: " + outl[0]);

//            int j = 0;
//            for (int k = bytesWritten; k < (8 + bytesWritten); k++) {
//                reduceGroupByRes[k] = idBytes[j];
//                j++;
//            }
//
//            doubleToByte(outd[0], reduceGroupByRes, bytesWritten + 8);
//            doubleToByte(outd2[0], reduceGroupByRes, (bytesWritten + 16));
//            longToByte(outl[0], reduceGroupByRes, (bytesWritten + 24));
//
//            bytesWritten += returnSize;
        }
        // 32 is fixed in this case since the kernel is precompiled
//        int returnSizeRes = reduceGroupByRes.length / 32;
//        AccelerationData ac = new AccelerationData(reduceGroupByRes, returnSizeRes);
//        conf.setAccelerationData(ac);
//        output.collect(null);
    }

}
