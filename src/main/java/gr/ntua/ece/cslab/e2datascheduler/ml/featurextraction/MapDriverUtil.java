package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction;

import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map.TornadoMap;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map.TornadoMap2;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map.TornadoMap3;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.AccelerationData;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.operators.util.TaskConfig;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.api.flink.FlinkData;

import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.examineTypeInfoForFlinkUDFs;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.setTypeVariablesForSecondInput;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.transformUDF;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.transformUDF2;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.transformUDF3;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.ExamineUDF.inOwner;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.ExamineUDF.outOwner;


/**
 * TODO(ckatsak): Documentation
 */
public class MapDriverUtil {

    // --------------------------------------------------------------------------------------------


    private final MapFunction function;

    public MapDriverUtil(final MapFunction mapFunction) {
        this.function = mapFunction;
    }


    // --------------------------------------------------------------------------------------------


    public void fakeCompile(final Configuration conf) {
        // examine udf
//        TaskConfig taskConf = this.taskContext.getTaskConfig();
//        Configuration conf = taskConf.getConfiguration();
        int predNum = conf.getNumberOfPredecessors();
        int numOfElements = 0;
        byte[] inputByteData = null;
        byte[] broadcastedBytes = null;
        int broadcastedSize = 0;
        boolean broadcastedDataset = false;
        TypeInformation broadcastedTypeInfo = null;
        if (predNum == 1) {
            Configuration predConf = conf.getPredecessorConfig(0);
            AccelerationData acdata = (AccelerationData) predConf.getAccelerationData();
//            AccelerationData acdata = new AccelerationData();
            numOfElements = acdata.getInputSize();
            inputByteData = acdata.getRawData();
        } else {
            broadcastedDataset = true;
            Configuration firstPredConf = conf.getPredecessorConfig(0);
            AccelerationData acdata = (AccelerationData) firstPredConf.getAccelerationData();
//            AccelerationData acdata = new AccelerationData();
            if (acdata.isBroadcasted()) {
                broadcastedBytes = acdata.getRawData();
                broadcastedSize = acdata.getInputSize();
                broadcastedTypeInfo = acdata.getTypeInfo();
                Configuration secondPredConf = conf.getPredecessorConfig(1);
                AccelerationData acdata2 = (AccelerationData) secondPredConf.getAccelerationData();
//                AccelerationData acdata2 = new AccelerationData();
                numOfElements = acdata2.getInputSize();
                inputByteData = acdata2.getRawData();
            } else {
                numOfElements = acdata.getInputSize();
                inputByteData = acdata.getRawData();
                Configuration secondPredConf = conf.getPredecessorConfig(1);
                AccelerationData broadcastedAcdata = (AccelerationData) secondPredConf.getAccelerationData();
//                AccelerationData broadcastedAcdata = new AccelerationData();
                broadcastedBytes = broadcastedAcdata.getRawData();
                broadcastedSize = broadcastedAcdata.getInputSize();
                broadcastedTypeInfo = broadcastedAcdata.getTypeInfo();
            }

        }

        String mapUserClassName = function.getClass().toString().replace("class ", "").replace(".", "/");
        TornadoMap msk = transformUDF(mapUserClassName);

        switch (inOwner) {
            case "java/lang/Integer": {
                int[] in = new int[numOfElements];
                // Based on the output data type create the output array and the Task Schedule
                // Currently, the input and output data can only be of type Integer, Double, Float or Long.
                // Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
                // since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.
                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];

                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        // the results in bytes
                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];

                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];

                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                }
                break;
            }
            case "java/lang/Double": {
                double[] in = new double[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                }
                break;
            }
            case "java/lang/Float": {
                float[] in = new float[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                }

                break;
            }
            case "java/lang/Long": {
                long[] in = new long[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        AccelerationData acres = new AccelerationData(outT, numOfElementsRes, 8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                }
                break;
            }
            case "org/apache/flink/api/java/tuple/Tuple2": {
                Tuple2[] in = new Tuple2[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(int.class);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();

                        fct0.setStoreJavaKind(double.class);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(8);
//                        conf.setAccelerationData(acres);

                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(float.class);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(long.class);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "org/apache/flink/api/java/tuple/Tuple2": {
                        Tuple2[] out = new Tuple2[numOfElements];
                        int returnSize = 0;

                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }
                        if (broadcastedDataset) {
                            setTypeVariablesForSecondInput(broadcastedTypeInfo, fct0);
                            fct0.setBroadcastedDataset(true);
                            if (broadcastedTypeInfo.toString().contains("Tuple3")) {
                                Tuple3[] in2 = new Tuple3[broadcastedSize];
                                byte[] outBytes = new byte[numOfElements * returnSize];
                                FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);

                                new TaskSchedule("s0")
                                        .flinkCompilerData(fct0)
                                        .flinkInfo(f)
                                        .task("t0", msk::map, in, in2, out)
                                        .streamOut(outBytes)
                                        .execute();

                                //byte[] outT = f.getByteResults();
//                                System.out.println("---- SELECTNEAREST OUTPUT BYTES ----");
//                                for (int i = 0; i < outBytes.length; i++) {
//                                    System.out.print(outBytes[i] + " ");
//                                }
//                                System.out.println();

//                                int numOfElementsRes = outBytes.length / returnSize;
//                                acres.setRawData(outBytes);
//                                acres.setInputSize(numOfElementsRes);
//                                acres.setReturnSize(returnSize);
//                                conf.setAccelerationData(acres);
                            } else if (broadcastedTypeInfo.toString().contains("Tuple2")) {
                                // code for exus here;
                                Tuple2[] in2 = new Tuple2[broadcastedSize];

                                int size = 0;
                                int arrayPos = -1;
                                byte[] outBytes;
                                if (fct0.getReturnArrayField()) {
                                    size = fct0.getReturnArrayFieldTotalBytes();
                                    arrayPos = fct0.getReturnTupleArrayFieldNo();
                                    for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
                                        if (i != arrayPos) {
                                            if (fct0.getDifferentTypesRet()) {
                                                size += 8;
                                            } else {
                                                size += fct0.getFieldSizesRet().get(i);
                                            }
                                        }
                                    }
                                    outBytes = new byte[numOfElements * size];
                                } else {
                                    outBytes = new byte[numOfElements * returnSize];
                                }
                                //returnSize = 680;
                                //byte[] outBytes = new byte[numOfElements * returnSize];
                                FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);

                                new TaskSchedule("s0")
                                        .flinkCompilerData(fct0)
                                        .flinkInfo(f)
                                        .task("t0", msk::map, in, in2, out)
                                        .streamOut(outBytes)
                                        .execute();

                                //byte[] outT = f.getByteResults();
//							System.out.println("---- EXUS MAP OUTPUT BYTES ----");
//							for (int i = 0; i < outBytes.length; i++) {
//								System.out.print(outBytes[i] + " ");
//							}
//							System.out.println();

//                                int numOfElementsRes = outBytes.length / returnSize;
//                                acres.setRawData(outBytes);
//                                acres.setInputSize(numOfElementsRes);
//                                acres.setReturnSize(returnSize);
//                                if (fct0.getReturnArrayField()) {
//                                    acres.hasArrayField();
//                                    acres.setLengthOfArrayField(83);
//                                    acres.setArrayFieldNo(0);
//                                    int totalB = fct0.getReturnArrayFieldTotalBytes();
//                                    for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
//                                        if (i != arrayPos) {
//                                            totalB += fct0.getFieldSizesRet().get(i);
//                                        }
//                                    }
//                                    acres.setTotalBytes(totalB);
//                                    acres.setRecordSize(size);
//                                }
//                                conf.setAccelerationData(acres);
                            }
                        } else {
                            byte[] outBytes = new byte[numOfElements * returnSize];
                            FlinkData f = new FlinkData(inputByteData, outBytes);

                            new TaskSchedule("s0")
                                    .flinkCompilerData(fct0)
                                    .flinkInfo(f)
                                    .task("t0", msk::map, in, out)
                                    .streamOut(outBytes)
                                    .execute();

                            //byte[] outT = f.getByteResults();
//                            int numOfElementsRes = outBytes.length / returnSize;
//                            acres.setRawData(outBytes);
//                            acres.setInputSize(numOfElementsRes);
//                            acres.setReturnSize(returnSize);
//                            conf.setAccelerationData(acres);
                        }

                        break;
                    }
                    case "org/apache/flink/api/java/tuple/Tuple3": {
                        Tuple3[] out = new Tuple3[numOfElements];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        AccelerationData acres = new AccelerationData();

                        int returnSize = 0;
                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        TornadoMap2 msk2 = transformUDF2(mapUserClassName);

                        byte[] outBytes = new byte[numOfElements * returnSize];

                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk2::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

                        //TornadoRuntime.getTornadoRuntime().getDriver(0).getDevice(deviceIndex).reset();

//                        int numOfElementsRes = outBytes.length / returnSize;
//                        acres.setRawData(outBytes);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(returnSize);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "org/apache/flink/api/java/tuple/Tuple4": {
                        Tuple4[] out = new Tuple4[numOfElements];
                        int returnSize = 0;

                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }
                        if (broadcastedDataset) {
                            setTypeVariablesForSecondInput(broadcastedTypeInfo, fct0);
                            fct0.setBroadcastedDataset(true);
                            if (broadcastedTypeInfo.toString().contains("Tuple2")) {
                                // code for exus here;
                                Tuple2[] in2 = new Tuple2[broadcastedSize];

                                int size = 0;
                                int arrayPos = -1;
                                byte[] outBytes;
                                if (fct0.getReturnArrayField()) {
                                    size = fct0.getReturnArrayFieldTotalBytes();
                                    arrayPos = fct0.getReturnTupleArrayFieldNo();
                                    for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
                                        if (i != arrayPos) {
                                            if (fct0.getDifferentTypesRet()) {
                                                size += 8;
                                            } else {
                                                size += fct0.getFieldSizesRet().get(i);
                                            }
                                        }
                                    }
                                    outBytes = new byte[numOfElements * size];
                                } else {
                                    outBytes = new byte[numOfElements * returnSize];
                                }
                                //returnSize = 680;
                                //byte[] outBytes = new byte[numOfElements * returnSize];
                                FlinkData f = new FlinkData(inputByteData, broadcastedBytes, outBytes);
//                                System.out.println("EXUS");
                                new TaskSchedule("s0")
                                        .flinkCompilerData(fct0)
                                        .flinkInfo(f)
                                        .task("t0", msk::map, in, in2, out)
                                        .streamOut(outBytes)
                                        .execute();

                                //byte[] outT = f.getByteResults();
//                                System.out.println("---- EXUS MAP OUTPUT BYTES ----");
//                                for (int i = 0; i < outBytes.length; i++) {
//                                    System.out.print(outBytes[i] + " ");
//                                }
//                                System.out.println();

//                                int numOfElementsRes = outBytes.length / returnSize;
//                                acres.setRawData(outBytes);
//                                acres.setInputSize(numOfElementsRes);
//                                acres.setReturnSize(returnSize);
//                                if (fct0.getReturnArrayField()) {
//                                    acres.hasArrayField();
//                                    acres.setLengthOfArrayField(83);
//                                    acres.setArrayFieldNo(0);
//                                    int totalB = fct0.getReturnArrayFieldTotalBytes();
//                                    for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
//                                        if (i != arrayPos) {
//                                            totalB += fct0.getFieldSizesRet().get(i);
//                                        }
//                                    }
//                                    acres.setTotalBytes(totalB);
//                                    acres.setRecordSize(size);
//                                }
//                                conf.setAccelerationData(acres);
                            }
                        }
                        break;
                    }
                }

                break;
            }
            case "org/apache/flink/api/java/tuple/Tuple3": {
                Tuple3[] in = new Tuple3[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(int.class);

                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();
//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(double.class);

                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();
//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(long.class);

                        byte[] outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess4(outBytes);
//                        int numOfElementsRes = outT.length / 4;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];

                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(long.class);

                        byte[] outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();

//                        byte[] outT = changeOutputEndianess8(outBytes);
//                        int numOfElementsRes = outT.length / 8;
//                        acres.setRawData(outT);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "org/apache/flink/api/java/tuple/Tuple3": {
                        Tuple3[] out = new Tuple3[numOfElements];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        AccelerationData acres = new AccelerationData();

                        int returnSize = 0;
                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(function.getClass().toString().replace("class ", ""))) {
                                returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }
                        byte[] outBytes = new byte[numOfElements * returnSize];

                        TornadoMap3 msk3 = transformUDF3(mapUserClassName);

                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk3::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        //byte[] outT = f.getByteResults();
//                        int numOfElementsRes = outBytes.length / returnSize;
//                        acres.setRawData(outBytes);
//                        acres.setInputSize(numOfElementsRes);
//                        acres.setReturnSize(returnSize);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                }

                break;
            }
            case "org/apache/flink/api/java/tuple/Tuple4": {
                Tuple4[] in = new Tuple4[numOfElements];

                if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();

                    Tuple3[] out = new Tuple3[numOfElements];
                    AccelerationData acres = new AccelerationData();
                    int returnSize = 0;
                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(function.getClass().toString().replace("class ", ""))) {
                            returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                            break;
                        }
                    }

                    byte[] outBytes = new byte[numOfElements * returnSize];
                    FlinkData f = new FlinkData(inputByteData, outBytes);

                    new TaskSchedule("s0")
                            .flinkCompilerData(fct0)
                            .flinkInfo(f)
                            .task("t0", msk::map, in, out)
                            .streamOut(outBytes)
                            .execute();

                    //byte[] outT = f.getByteResults();
//                    int numOfElementsRes = outBytes.length / returnSize;
//                    acres.setRawData(outBytes);
//                    acres.setInputSize(numOfElementsRes);
//                    acres.setReturnSize(returnSize);
//                    conf.setAccelerationData(acres);
                } else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
                    Tuple4[] out = new Tuple4[numOfElements];
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                    AccelerationData acres = new AccelerationData();

                    int returnSize = 0;
                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(function.getClass().toString().replace("class ", ""))) {
                            returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                            break;
                        }
                    }

                    byte[] outBytes = new byte[numOfElements * returnSize];
                    FlinkData f = new FlinkData(inputByteData, outBytes);
                    new TaskSchedule("s0")
                            .flinkCompilerData(fct0)
                            .flinkInfo(f)
                            .task("t0", msk::map, in, out)
                            .streamOut(outBytes)
                            .execute();

                    //byte[] outT = f.getByteResults();
//                    int numOfElementsRes = outBytes.length / returnSize;
//                    acres.setRawData(outBytes);
//                    acres.setInputSize(numOfElementsRes);
//                    acres.setReturnSize(returnSize);
//                    conf.setAccelerationData(acres);
                }
                break;
            }
        }
//        output.collect(null);
    }

}
