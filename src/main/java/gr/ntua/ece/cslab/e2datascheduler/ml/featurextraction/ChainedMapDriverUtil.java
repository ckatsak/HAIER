package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction;

import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map.TornadoMap;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.FlinkCompilerInfo;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.FlinkData;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import uk.ac.manchester.tornado.api.TaskSchedule;

/**
 * TODO(ckatsak): Documentation
 */
public class ChainedMapDriverUtil {

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


    //--------------------------------------------------------------------------------------------


    public static void fakeCompile(final MapFunction mapper) {

    }

}
