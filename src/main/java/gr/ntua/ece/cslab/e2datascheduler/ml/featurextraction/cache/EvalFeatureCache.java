package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.cache;

import gr.ntua.ece.cslab.e2datascheduler.beans.cluster.HwResource;
import gr.ntua.ece.cslab.e2datascheduler.graph.HaierExecutionGraph;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.FeatureCache;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoFeatureVector;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class EvalFeatureCache implements FeatureCache {

    private static final Logger logger = Logger.getLogger(EvalFeatureCache.class.getCanonicalName());

    private final Map<JobVertex, Map<HwResource, List<TornadoFeatureVector>>> cacheEntryMap;

    public EvalFeatureCache(final JobGraph jobGraph, final List<HwResource> devices) {
        this.cacheEntryMap = new HashMap<>(jobGraph.getNumberOfVertices());
        for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
            if (!HaierExecutionGraph.isComputational(jobVertex)) {
                logger.finer("Skipping JobVertex '" + jobVertex.getID().toString() + "' (named '" +
                        jobVertex.getName() + "') as non-computational");
                continue;
            }

            logger.finest("Examining JobVertex '" + jobVertex.getID().toString() + "' (named '" +
                    jobVertex.getName() + "')...");
            final String[] jobVertexOperators = jobVertex.getName().split(" -> ");
            final List<TornadoFeatureVector> jobVertexKernels = new ArrayList<>(jobVertexOperators.length);
            for (String operator : jobVertexOperators) {
                final String kernelName;
                if (operator.contains("SparkWorksAllReduce.java:73")) {
                    kernelName = "prebuilt-sparkworks-reduce-average.cl";
                } else if (operator.contains("SparkWorksAllReduce.java:78")) {
                    kernelName = "prebuilt-sparkworks-reduce-max.cl";
                } else if (operator.contains("SparkWorksAllReduce.java:79")) {
                    kernelName = "prebuilt-sparkworks-reduce-sum.cl";
                } else if (operator.contains("SparkWorksAllReduce.java:80")) {
                    kernelName = "prebuilt-sparkworks-reduce-min.cl";
                } else if (operator.contains("ExusFlinkTornado.java:47")) {
                    kernelName = "prebuilt-exus-map-reduction.cl";
                } else if (operator.contains("ExusFlinkTornado.java:48")) {
                    kernelName = "prebuilt-exus-reduction-UpdateAccum.cl";
                } else {
                    logger.warning("Unknown Operator named '" + operator + "' not supported in this execution mode");
                    continue;
                }
                logger.finest("Adding new Operator '" + kernelName + "' for JobVertex " + jobVertex.getID());
                jobVertexKernels.add(new TornadoFeatureVector(kernelName));
            }

            this.cacheEntryMap.put(jobVertex, new HashMap<>(devices.size()));
            for (HwResource device : devices) {
                logger.finer("Feature vector for JobVertex '" + jobVertex.getID() + "' on OpenCL-enabled device '" +
                        device.getName() + " @ " + device.getHost() + "' is cached for later access");
                this.cacheEntryMap.get(jobVertex).put(device, jobVertexKernels);
            }
        }
    }

    /**
     * TODO(ckatsak): Documentation
     *
     * @param jobVertex
     * @param device
     * @return
     */
    @Override
    public List<TornadoFeatureVector> getFeatureVectors(final JobVertex jobVertex, final HwResource device) {
        logger.finest("Retrieving cached feature vector for JobVertex '" + jobVertex.getID() +
                "' on OpenCL-enabled device '" + device.getName() + " @ " + device.getHost() + "'...");
//        logger.finest("Feature cache contents: " + this.cacheEntryMap.toString());
        // Non-computational JobVertices won't be cached, but they shouldn't be queried either, so just
        // store null in their ScheduledJobVertex's TornadoFeatureVector field.
        if (null == this.cacheEntryMap.get(jobVertex)) {
            return null;
        }
        return this.cacheEntryMap.get(jobVertex).get(device);
    }
}
