package gr.ntua.ece.cslab.e2datascheduler.ws;

import gr.ntua.ece.cslab.e2datascheduler.E2dScheduler;
import gr.ntua.ece.cslab.e2datascheduler.beans.graph.SerializableScheduledJobVertex;
import gr.ntua.ece.cslab.e2datascheduler.beans.gui.CandidatePlan;
import gr.ntua.ece.cslab.e2datascheduler.beans.optpolicy.OptimizationPolicy;
import gr.ntua.ece.cslab.e2datascheduler.graph.HaierExecutionGraph;
import gr.ntua.ece.cslab.e2datascheduler.optimizer.nsga.NSGAIIParameters;
import gr.ntua.ece.cslab.e2datascheduler.util.HaierLogHandler;
import gr.ntua.ece.cslab.e2datascheduler.util.SelectionQueue;

import org.apache.flink.runtime.jobgraph.JobGraph;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.GsonBuilder;

import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * REST API of {@link E2dScheduler}
 * Can be used for submitting and scheduling applications
 */
@Path("/e2data")
public class SchedulerService extends AbstractE2DataService {

    private static final Logger logger = Logger.getLogger(SchedulerService.class.getCanonicalName());

    public static final ResourceBundle resourceBundle = ResourceBundle.getBundle("config");
    private static final String tmpRootPath = resourceBundle.getString("haier.tmp.path");

    private final E2dScheduler scheduler;


    // --------------------------------------------------------------------------------------------


    public SchedulerService() {
        scheduler = E2dScheduler.getInstance();
    }

    // just for testing setup
    @Path("/hello")
    @GET
    public Response hello() {
        String happyMesg = "hello";

        return generateResponse(Response.Status.OK, happyMesg);
    }


    // --------------------------------------------------------------------------------------------


    @Path("/nsga2/params")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getNSGAIIParameters() {
        logger.finest("Just received a GET request on /e2data/nsga2/params !");

        return generateResponse(Response.Status.OK, this.scheduler.retrieveConfiguration());
    }

    @Path("/nsga2/params")
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response setNSGAIIParameters(final NSGAIIParameters parameters) {
        logger.finest("Just received a PUT request on /e2data/nsga2/params !");

        if (null == parameters) {
            logger.info("NSGAIIParameters received by GUI is null");
            return generateResponse(Response.Status.BAD_REQUEST, "");
        }

        this.scheduler.configureOptimizer(parameters);
        return generateResponse(Response.Status.NO_CONTENT, "");
    }


    // --------------------------------------------------------------------------------------------


    @Path("/nsga2/{jobId}/plans")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlans(@PathParam("jobId") final String jobId) {
        logger.finest("Just received a GET request on /e2data/nsga2/" + jobId + "/plans !");

        // Retrieve Job's SelectionQueue. If it doesn't exist (just yet, possibly) return 404 NOT FOUND.
        final SelectionQueue<CandidatePlan> plansQueue = this.scheduler.getSelectionQueue(jobId);
        if (null == plansQueue) {
            return generateResponse(Response.Status.NOT_FOUND, "");
        }

        // If the SelectionQueue already exists (i.e., the Job in question is already being tracked by HAIER),
        // make an attempt to retrieve all execution plans calculated by the NSGAIIOptimizer.
        List<CandidatePlan> candidatePlans = null;
        try {
            candidatePlans = plansQueue.retrieveOptions(500);
        } catch (InterruptedException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
            return generateResponse(Response.Status.INTERNAL_SERVER_ERROR, "InterruptedException");
        }

        // If no execution plans are available after 500ms return 202 ACCEPTED to prompt the GUI to retry later.
        if (null == candidatePlans) {
            logger.info("Candidate execution plans for Job " + jobId + " appear not to be ready to be sent yet.");
            return generateResponse(Response.Status.ACCEPTED, "");
        }
        logger.info("The list of candidate execution plans is being sent.");
        return generateResponse(Response.Status.OK, candidatePlans);
    }

    @Path("/nsga2/{jobId}/plans")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response setPlan(@PathParam("jobId") final String jobId, final CandidatePlan candidatePlan) {
        logger.finest("Just received a POST request on /e2data/nsga2/" + jobId + "/plans !");

        // Make sure the input CandidatePlan is OK.
        if (null == candidatePlan) {
            logger.warning("CandidatePlan sent is null.");
            return generateResponse(Response.Status.BAD_REQUEST, "");
        }
        logger.finer("CandidatePlan selected by the GUI: " + candidatePlan);

        // Retrieve Job's SelectionQueue. It should definitely exist by now (since a GET must have been preceded),
        // hence the error in case of failure.
        final SelectionQueue<CandidatePlan> plansQueue = this.scheduler.getSelectionQueue(jobId);
        if (null == plansQueue) {
            logger.severe("SelectionQueue for job " + jobId + " could not be found.");
            return generateResponse(Response.Status.INTERNAL_SERVER_ERROR, "");
        }

        plansQueue.submitChoice(candidatePlan);
        return generateResponse(Response.Status.NO_CONTENT, "");
    }


    // --------------------------------------------------------------------------------------------


    @Path("/logs")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLogs() {
        logger.finest("Just received a GET request on /e2data/logs !");

        final BlockingQueue<String> logsQueue = HaierLogHandler.getLogsQueue();
        final List<String> readyLogs = new LinkedList<>();
        while (null != logsQueue.peek()) {
            final String logMsg = logsQueue.poll();
            readyLogs.add(Base64.getEncoder().encodeToString(logMsg.getBytes()));
        }

        return generateResponse(Response.Status.OK, readyLogs);
    }


    // --------------------------------------------------------------------------------------------


    @Path("/flink-schedule")
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response flink_schedule(
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetail) {
        logger.finest("Just received a POST request on /e2data/flink-schedule !");

        final String filePath = tmpRootPath + fileDetail.getFileName();
        try {
            SchedulerService.persistSerializedJobGraphFile(uploadedInputStream, filePath);
            logger.info("JobGraph file '" + fileDetail.getFileName() +
                    "' has been uploaded successfully and stored in '" + filePath + "'.");
        } catch (IOException e) {
            logger.warning("Error retrieving JobGraph file '" + fileDetail.getFileName() + "':" + e.getMessage());
            e.printStackTrace();
            return generateResponse(Response.Status.INTERNAL_SERVER_ERROR,
                                    "Error retrieving JobGraph file '" + fileDetail.getFileName() + "'.");
        }

        JobGraph jobGraph;
        try (final ObjectInputStream objectIn = new ObjectInputStream(new FileInputStream(filePath))) {
            jobGraph = (JobGraph) objectIn.readObject();
            logger.info("Deserialized '" + jobGraph + "' from file '" + filePath + "'.");
        } catch (Exception e) {
            logger.warning("Error deserializing JobGraph from file '" + filePath + "': " + e.getMessage());
            e.printStackTrace();
            return generateResponse(Response.Status.INTERNAL_SERVER_ERROR,
                                    "Error deserializing JobGraph from file '" + fileDetail.getFileName() + "'.");
        } finally {
            if (!new File(filePath).delete()) {
                logger.warning("Error unlinking JobGraph file '" + filePath + "'.");
            }
        }

        // TODO(ckatsak): For now, a default OptimizationPolicy object is
        //                hardcoded here instead of being sent by Flink.
        //final String policyStr = "{\"policy\": {\"objectives\": [ {\"name\":\"execTime\", \"targetFunction\":\"MIN\", \"combineFunction\":\"MAX\"}, {\"name\":\"powerCons\", \"targetFunction\":\"MIN\", \"combineFunction\":\"SUM\"} ]}}";
        final String policyStr = "{" +
                            "\"policy\": {" +
                                "\"objectives\": [" +
                                    "{" +
                                        "\"name\": \"execTime\"," +
                                        "\"combineFunction\": \"MAX\"," +
                                        "\"targetFunction\": \"MIN\"" +
                                    "}," +
                                    "{" +
                                        "\"name\": \"powerCons\"," +
                                        "\"combineFunction\": \"SUM\"," +
                                        "\"targetFunction\": \"MIN\"" +
                                    "}" +
                                "]" +
                            "}" +
                        "}";

        final E2dScheduler.SchedulingResult result =
                this.scheduler.schedule(jobGraph, OptimizationPolicy.parseJSON(policyStr));
        if (null == result) {
            logger.severe("E2dScheduler.SchedulingResult is null; something serious is going on!");
            return generateResponse(Response.Status.INTERNAL_SERVER_ERROR, "");
        }
        if (result.isEmpty()) {
            logger.info("It looks like none of the JobVertices in '" + jobGraph.toString() +
                    "' can be offloaded to an accelerator.");
            return generateResponse(Response.Status.OK, new ArrayList<>(0));
        }
        final HaierExecutionGraph resultGraph = result.getResult();
        if (null == resultGraph) {
            logger.severe("E2dScheduler.SchedulingResult.getResult() returned null!");
            logger.severe("Error allocating resources for '" + jobGraph.toString() + "'");
            return generateResponse(Response.Status.INTERNAL_SERVER_ERROR, "");
        }

        logger.info("Scheduling result for '" + jobGraph.toString() + "':\n" + result);

        final List<SerializableScheduledJobVertex> responseBody = resultGraph.toSerializableScheduledJobVertexList();
        logger.finest("Response body returned to Flink for '" + jobGraph.toString() + "':\n" +
                new GsonBuilder().setPrettyPrinting().create().toJson(responseBody));
        return generateResponse(Response.Status.OK, responseBody);
    }

    private static void persistSerializedJobGraphFile(
            final InputStream uploadedInputStream,
            final String filePath)
            throws IOException {
        final FileOutputStream out = new FileOutputStream(new File(filePath));
        final byte[] bytes = new byte[8192];
        int read;
        while ((read = uploadedInputStream.read(bytes)) != -1) {
            logger.finest("Read " + read + " bytes from the uploaded serialized JobGraph.");
            out.write(bytes, 0, read);
        }
        out.flush();
        out.close();
    }

}

