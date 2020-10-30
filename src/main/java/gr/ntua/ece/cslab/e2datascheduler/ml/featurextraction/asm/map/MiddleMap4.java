package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.asm.map;

import org.apache.flink.api.java.tuple.Tuple3;


/**
 * The purpose of this abstract class is to enable us to use the newly loaded
 * class like any other regular java class, using casting.
 * This is necessary because if we were to use other techniques, like reflection, we
 * wouldn't be able to pass the ASM generated class to the Task Schedules of Tornado.
 */
public abstract class MiddleMap4 {

    public abstract Tuple3 mymaptuple3tuple3(Tuple3 i);

}
