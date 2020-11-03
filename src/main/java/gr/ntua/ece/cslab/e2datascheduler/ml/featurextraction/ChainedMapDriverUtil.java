package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction;

import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map.TornadoMap;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map.TornadoMap2;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map.TornadoMap3;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.AccelerationData;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import uk.ac.manchester.tornado.api.TaskSchedule;
import uk.ac.manchester.tornado.api.flink.FlinkCompilerInfo;
import uk.ac.manchester.tornado.api.flink.FlinkData;

import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.examineTypeInfoForFlinkUDFs;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.transformUDF2;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoUtil.transformUDF3;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.ExamineUDF.inOwner;
import static gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.ExamineUDF.outOwner;


/**
 * TODO(ckatsak): Documentation
 */
public class ChainedMapDriverUtil {

    private static final int INT_LENGTH = 4;


    // --------------------------------------------------------------------------------------------


    private final byte[] inputByteData;
    private final int numOfElements;

    private final MapFunction mapper;

    public ChainedMapDriverUtil(final MapFunction mapFunction) {
        this.mapper = mapFunction;
        this.inputByteData = new byte[]{};
        this.numOfElements = 42;
    }


    // --------------------------------------------------------------------------------------------


    private void createTaskScheduleAndRun(TornadoMap msk, int[] in, int[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, int[] in, float[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, int[] in, double[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, int[] in, long[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, double[] in, int[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, double[] in, float[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, double[] in, double[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, double[] in, long[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, long[] in, int[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, long[] in, float[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, long[] in, double[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, long[] in, long[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, float[] in, int[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, float[] in, float[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, float[] in, double[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, float[] in, long[] out, byte[] outBytes, FlinkData f) {
        new TaskSchedule("s0")
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }

    private void createTaskScheduleAndRun(TornadoMap msk, Tuple2[] in, int[] out, byte[] outBytes, FlinkData f, FlinkCompilerInfo fct0) {
        new TaskSchedule("s0")
                .flinkCompilerData(fct0)
                .flinkInfo(f)
                .task("t0", msk::map, in, out)
                .streamOut(outBytes)
                .execute();
    }


    // --------------------------------------------------------------------------------------------


    private abstract static class TypeBytes {
        protected int bytes = 0;
        public int getBytes() {
            return bytes;
        }
    }

    private static class TInteger extends TypeBytes {
        public TInteger() {
            bytes = 4;
        }
    }

    private static class TFloat extends TypeBytes {
        public TFloat() {
            bytes = 4;
        }
    }

    private static class TLong extends TypeBytes {
        public TLong() {
            bytes = 8;
        }
    }

    private static class TDouble extends TypeBytes {
        public TDouble() {
            bytes = 8;
        }
    }


    //--------------------------------------------------------------------------------------------


    private AccelerationData examineType(FlinkCompilerInfo fct0) {
        AccelerationData acres = new AccelerationData();
        for (String name : TornadoUtil.typeInfo.keySet()) {
            if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
                examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                break;
            }
        }
        return acres;
    }


    // --------------------------------------------------------------------------------------------


    /**
     * FIXME(ckatsak): Implementation? Documentation
     *
     * The only public API (along with the constructor)
     *
     */
    public void fakeCompile() {
//        Configuration conf = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig();

        // check number of predecessors to see if we have broadcasted input
//        AccelerationData accData = (AccelerationData) conf.getAccelerationData();
        AccelerationData accData = new AccelerationData();

        // Input data to the Map Operator
//        inputByteData = accData.getRawData();
//        byte[] inputByteData = new byte[]{};

        // Number of elements to be processed
//        numOfElements = accData.getInputSize();
//        int numOfElements = 42;

        String mapUserClassName = this.mapper.getClass().toString().replace("class ", "").replace(".", "/");
        TornadoMap msk = TornadoUtil.transformUDF(mapUserClassName);

        TypeBytes typeBytes = null;
        FlinkData flinkData = null;
        byte[] outBytes = null;

        switch (inOwner) {
            case "java/lang/Integer": {
                int[] in = new int[numOfElements];
                /* Based on the output data type create the output array and the Task Schedule
                 * Currently, the input and output data can only be of type Integer, Double, Float or Long.
                 * Note that it is safe to test only these cases at this point of execution because if the types were Java Objects,
                 * since the integration does not support POJOs yet, setTypeVariablesMap would have already thrown an exception.
                 */
                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        outBytes = new byte[numOfElements * INT_LENGTH];
                        typeBytes = new TInteger();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TDouble();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        typeBytes = new TFloat();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TLong();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                }
                outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) :  changeOutputEndianess8(outBytes);
                int numOfElementsRes = outBytes.length / typeBytes.getBytes();
                AccelerationData acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
//                conf.setAccelerationData(acres);
                break;
            }
            case "java/lang/Double": {
                double[] in = new double[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        typeBytes = new TInteger();
                        outBytes = new byte[numOfElements * 4];
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TDouble();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        typeBytes = new TFloat();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TLong();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                }
                outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) :  changeOutputEndianess8(outBytes);
                int numOfElementsRes = outBytes.length / typeBytes.getBytes();
                AccelerationData acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
//                conf.setAccelerationData(acres);
                break;
            }
            case "java/lang/Float": {
                float[] in = new float[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        typeBytes = new TInteger();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TDouble();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        typeBytes = new TFloat();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TLong();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                }
                outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
                int numOfElementsRes = outBytes.length / typeBytes.getBytes();
                AccelerationData acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
//                conf.setAccelerationData(acres);
                break;
            }
            case "java/lang/Long": {
                long[] in = new long[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        typeBytes = new TInteger();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TDouble();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        typeBytes = new TFloat();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        typeBytes = new TLong();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData);
                        break;
                    }
                }
                outBytes = (typeBytes.getBytes() == 4) ? changeOutputEndianess4(outBytes) : changeOutputEndianess8(outBytes);
                int numOfElementsRes = outBytes.length / typeBytes.getBytes();
                AccelerationData acres = new AccelerationData(outBytes, numOfElementsRes, typeBytes.getBytes());
//                conf.setAccelerationData(acres);
                break;
            }
            case "org/apache/flink/api/java/tuple/Tuple2": {
                Tuple2[] in = new Tuple2[numOfElements];

                switch (outOwner) {
                    case "java/lang/Integer": {
                        int[] out = new int[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        typeBytes = new TInteger();
                        flinkData = new FlinkData(inputByteData, outBytes);
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(int.class);

                        AccelerationData acres = examineType(fct0);
                        createTaskScheduleAndRun(msk, in, out, outBytes, flinkData, fct0);

                        outBytes = changeOutputEndianess4(outBytes);
                        int numOfElementsRes = outBytes.length / typeBytes.getBytes();
                        acres.setRawData(outBytes);
                        acres.setInputSize(numOfElementsRes);
                        acres.setReturnSize(typeBytes.getBytes());
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Double": {
                        double[] out = new double[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(double.class);
                        AccelerationData acres = examineType(fct0);

                        flinkData = new FlinkData(inputByteData, outBytes);
                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(flinkData)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        outBytes = changeOutputEndianess8(outBytes);

                        int numOfElementsRes = outBytes.length / 8;
                        acres.setRawData(outBytes);
                        acres.setInputSize(numOfElementsRes);
                        acres.setReturnSize(8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Float": {
                        float[] out = new float[numOfElements];
                        outBytes = new byte[numOfElements * 4];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(float.class);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

                        byte[] outT = changeOutputEndianess4(outBytes);

                        int numOfElementsRes = outT.length / 4;
                        acres.setRawData(outT);
                        acres.setInputSize(numOfElementsRes);
                        acres.setReturnSize(4);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "java/lang/Long": {
                        long[] out = new long[numOfElements];
                        outBytes = new byte[numOfElements * 8];
                        FlinkData f = new FlinkData(inputByteData, outBytes);
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        fct0.setStoreJavaKind(long.class);
                        AccelerationData acres = new AccelerationData();

                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

                        byte[] outT = changeOutputEndianess8(outBytes);

                        int numOfElementsRes = outT.length / 8;
                        acres.setRawData(outT);
                        acres.setInputSize(numOfElementsRes);
                        acres.setReturnSize(8);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                    case "org/apache/flink/api/java/tuple/Tuple2": {
                        Tuple2[] out = new Tuple2[numOfElements];
                        int returnSize = 0;
                        AccelerationData acres = new AccelerationData();

                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
                                returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        int size = 0;
                        int arrayPos = -1;
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
                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        int numOfElementsRes = outBytes.length / returnSize;
                        acres.setRawData(outBytes);
                        acres.setInputSize(numOfElementsRes);
                        acres.setReturnSize(returnSize);
//                        conf.setAccelerationData(acres);
                        if (fct0.getReturnArrayField()) {
                            acres.hasArrayField();
                            acres.setLengthOfArrayField(accData.getLengthOfArrayField());
                            acres.setArrayFieldNo(accData.getArrayFieldNo());
                            int totalB = fct0.getReturnArrayFieldTotalBytes();
                            for (int i = 0; i < fct0.getFieldSizesRet().size(); i++) {
                                if (i != arrayPos) {
                                    totalB += fct0.getFieldSizesRet().get(i);
                                }
                            }
                            acres.setTotalBytes(totalB);
                            acres.setRecordSize(size);
                        }
                        break;
                    }
                    case "org/apache/flink/api/java/tuple/Tuple3": {
                        Tuple3[] out = new Tuple3[numOfElements];
                        FlinkCompilerInfo fct0 = new FlinkCompilerInfo();

                        int returnSize = 0;
                        AccelerationData acres = new AccelerationData();
                        for (String name : TornadoUtil.typeInfo.keySet()) {
                            if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
                                returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                                break;
                            }
                        }

                        TornadoMap2 msk2 = transformUDF2(mapUserClassName);

                        outBytes = new byte[numOfElements * returnSize];

                        FlinkData f = new FlinkData(inputByteData, outBytes);

                        new TaskSchedule("s0")
                                .flinkCompilerData(fct0)
                                .flinkInfo(f)
                                .task("t0", msk2::map, in, out)
                                .streamOut(outBytes)
                                .execute();

                        int numOfElementsRes = outBytes.length / returnSize;
                        acres.setRawData(outBytes);
                        acres.setInputSize(numOfElementsRes);
                        acres.setReturnSize(returnSize);
//                        conf.setAccelerationData(acres);
                        break;
                    }
                }

                break;
            }
            case "org/apache/flink/api/java/tuple/Tuple3": {
                Tuple3[] in = new Tuple3[numOfElements];

                if (outOwner.equals("java/lang/Integer")) {
                    int[] out = new int[numOfElements];
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                    fct0.setStoreJavaKind(int.class);

                    outBytes = new byte[numOfElements * 4];
                    FlinkData f = new FlinkData(inputByteData, outBytes);
                    AccelerationData acres = new AccelerationData();

                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

                    byte[] outT = changeOutputEndianess4(outBytes);

                    int numOfElementsRes = outT.length / 4;
                    acres.setRawData(outT);
                    acres.setInputSize(numOfElementsRes);
                    acres.setReturnSize(4);
//                    conf.setAccelerationData(acres);
                } else if (outOwner.equals("java/lang/Double")) {
                    double[] out = new double[numOfElements];
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                    fct0.setStoreJavaKind(double.class);

                    outBytes = new byte[numOfElements * 8];
                    FlinkData f = new FlinkData(inputByteData, outBytes);
                    AccelerationData acres = new AccelerationData();

                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

                    byte[] outT = changeOutputEndianess8(outBytes);

                    int numOfElementsRes = outT.length / 8;
                    acres.setRawData(outT);
                    acres.setInputSize(numOfElementsRes);
                    acres.setReturnSize(8);
//                    conf.setAccelerationData(acres);
                } else if (outOwner.equals("java/lang/Float")) {
                    float[] out = new float[numOfElements];
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                    fct0.setStoreJavaKind(long.class);

                    outBytes = new byte[numOfElements * 4];
                    FlinkData f = new FlinkData(inputByteData, outBytes);
                    AccelerationData acres = new AccelerationData();

                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

                    byte[] outT = changeOutputEndianess4(outBytes);

                    int numOfElementsRes = outT.length / 4;
                    acres.setRawData(outT);
                    acres.setInputSize(numOfElementsRes);
                    acres.setReturnSize(4);
//                    conf.setAccelerationData(acres);
                } else if (outOwner.equals("java/lang/Long")) {
                    long[] out = new long[numOfElements];

                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();
                    fct0.setStoreJavaKind(long.class);

                    outBytes = new byte[numOfElements * 8];
                    FlinkData f = new FlinkData(inputByteData, outBytes);
                    AccelerationData acres = new AccelerationData();

                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
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

                    byte[] outT = changeOutputEndianess8(outBytes);

                    int numOfElementsRes = outT.length / 8;
                    acres.setRawData(outT);
                    acres.setInputSize(numOfElementsRes);
                    acres.setReturnSize(8);
//                    conf.setAccelerationData(acres);
                } else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
                    Tuple3[] out = new Tuple3[numOfElements];
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();

                    int returnSize = 0;
                    AccelerationData acres = new AccelerationData();

                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
                            returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                            break;
                        }
                    }
                    outBytes = new byte[numOfElements * returnSize];

                    TornadoMap3 msk3 = transformUDF3(mapUserClassName);

                    FlinkData f = new FlinkData(inputByteData, outBytes);

                    new TaskSchedule("s0")
                            .flinkCompilerData(fct0)
                            .flinkInfo(f)
                            .task("t0", msk3::map, in, out)
                            .streamOut(outBytes)
                            .execute();

                    int numOfElementsRes = outBytes.length / returnSize;
                    acres.setRawData(outBytes);
                    acres.setInputSize(numOfElementsRes);
                    acres.setReturnSize(returnSize);
//                    conf.setAccelerationData(acres);
                }

                break;
            }
            case "org/apache/flink/api/java/tuple/Tuple4": {
                Tuple4[] in = new Tuple4[numOfElements];

                if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple3")) {
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();

                    Tuple3[] out = new Tuple3[numOfElements];
                    int returnSize = 0;
                    AccelerationData acres = new AccelerationData();

                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
                            returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                            break;
                        }
                    }

                    outBytes = new byte[numOfElements * returnSize];
                    FlinkData f = new FlinkData(inputByteData, outBytes);

                    new TaskSchedule("s0")
                            .flinkCompilerData(fct0)
                            .flinkInfo(f)
                            .task("t0", msk::map, in, out)
                            .streamOut(outBytes)
                            .execute();

                    int numOfElementsRes = outBytes.length / returnSize;
                    acres.setRawData(outBytes);
                    acres.setInputSize(numOfElementsRes);
                    acres.setReturnSize(returnSize);
//                    conf.setAccelerationData(acres);
                } else if (outOwner.equals("org/apache/flink/api/java/tuple/Tuple4")) {
                    Tuple4[] out = new Tuple4[numOfElements];
                    FlinkCompilerInfo fct0 = new FlinkCompilerInfo();

                    AccelerationData acres = new AccelerationData();
                    int returnSize = 0;
                    for (String name : TornadoUtil.typeInfo.keySet()) {
                        if (name.contains(mapper.getClass().toString().replace("class ", ""))) {
                            returnSize = examineTypeInfoForFlinkUDFs(TornadoUtil.typeInfo.get(name)[0], TornadoUtil.typeInfo.get(name)[1], fct0, acres);
                            break;
                        }
                    }

                    outBytes = new byte[numOfElements * returnSize];
                    FlinkData f = new FlinkData(inputByteData, outBytes);
                    new TaskSchedule("s0")
                            .flinkCompilerData(fct0)
                            .flinkInfo(f)
                            .task("t0", msk::map, in, out)
                            .streamOut(outBytes)
                            .execute();

                    int numOfElementsRes = outBytes.length / returnSize;
                    acres.setRawData(outBytes);
                    acres.setInputSize(numOfElementsRes);
                    acres.setReturnSize(returnSize);
//                    conf.setAccelerationData(acres);
                }
                break;
            }
        }
//        this.outputCollector.collect(null);
    }

    public static byte[] changeOutputEndianess4(byte[] output) {
        byte tmp;
        for (int i = 0; i < output.length; i += 4) {
            // swap 0 and 3
            tmp = output[i];
            output[i] = output[i + 3];
            output[i + 3] = tmp;
            // swap 1 and 2
            tmp = output[i + 1];
            output[i + 1] = output[i + 2];
            output[i + 2] = tmp;
        }
        return output;
    }

    public static byte[] changeOutputEndianess8(byte[] output) {
        byte tmp;
        for (int i = 0; i < output.length; i += 8) {
            // swap 0 and 7
            tmp = output[i];
            output[i] = output[i + 7];
            output[i + 7] = tmp;
            // swap 1 and 6
            tmp = output[i + 1];
            output[i + 1] = output[i + 6];
            output[i + 6] = tmp;
            // swap 2 and 5
            tmp = output[i + 2];
            output[i + 2] = output[i + 5];
            output[i + 5] = tmp;
            // swap 3 and 4
            tmp = output[i + 3];
            output[i + 3] = output[i + 4];
            output[i + 4] = tmp;
        }
        return output;
    }

}
