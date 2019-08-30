package gr.ntua.ece.cslab.e2datascheduler.ws;

import gr.ntua.ece.cslab.e2datascheduler.E2dScheduler;
import gr.ntua.ece.cslab.e2datascheduler.beans.SubmittedTask;
import gr.ntua.ece.cslab.e2datascheduler.beans.graph.ScheduledGraphNode;
import gr.ntua.ece.cslab.e2datascheduler.beans.optpolicy.OptimizationPolicy;
import gr.ntua.ece.cslab.e2datascheduler.beans.graph.ExecutionGraph;
import gr.ntua.ece.cslab.e2datascheduler.beans.graph.ToyJobGraph;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.logging.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * REST API of {@link E2dScheduler}
 * Can be used for submitting and scheduling applications
 */

@Path("/e2data")
public class SchedulerService extends AbstractE2DataService {

    private static final Logger logger = Logger.getLogger(SchedulerService.class.getCanonicalName());

    // FIXME(ckatsak): should be configurable rather than hard-coded, with a sane default (e.g. "/tmp/")
    private static final String tmpRootPath = "/home/users/ckatsak/haier_tmp/";

    private E2dScheduler scheduler;

    public SchedulerService(){
        scheduler = E2dScheduler.getInstance();
    }

    @Path("/schedule")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response schedule(SubmittedTask inputTask) {
        //TODO: @gmytil: Take care of error handling. What can go wrong during optimization?
        //FIXME: @gmytil: What is the appropriate HTTP status to return for each case?

        logger.info(inputTask.getPolicy().getObjectives()[0].getName());

        OptimizationPolicy policy = inputTask.getPolicy();
        ToyJobGraph inputGraph = inputTask.getJobgraph();

        logger.info(inputGraph.toString());
        inputGraph.index();


        ExecutionGraph result = scheduler.schedule(inputGraph, policy);
        for(ScheduledGraphNode sn : result.getExecutionGraph()){
            logger.info(sn.toString());
        }
        return generateResponse(Response.Status.OK, result);
    }

    // just for testing setup
    @Path("/hello")
    @GET
    public Response schedule() {
        String happyMesg = "hello";
        return generateResponse(Response.Status.OK, happyMesg);
    }

    @Path("/flink-schedule")
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response flink_schedule(
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetail) {

        final String filePath = this.tmpRootPath + fileDetail.getFileName();

        try {
            this.storeSerializedJobGraphFile(uploadedInputStream, filePath);
            logger.info("JobGraph file '" + fileDetail.getFileName() + "' has been uploaded successfully; stored as '" + filePath + "'.");
        } catch (IOException e) {
            logger.warning("Error uploading JobGraph file '" + fileDetail.getFileName() + "'");
            e.printStackTrace();
            return generateResponse(Response.Status.INTERNAL_SERVER_ERROR, "Error uploading JobGraph file '" + fileDetail.getFileName() + "'.");
        }

        try {
            // TODO(ckatsak): deserialize
        } finally {
            // TODO(ckatsak): delete temporarily stored jobgraph file
        }

        // TODO(ckatsak): schedule

        return generateResponse(Response.Status.OK, "File '" + fileDetail.getFileName() + "' has been uploaded successfully!");
    }

    private void storeSerializedJobGraphFile(final InputStream uploadedInputStream, final String filePath) throws IOException {
        final FileOutputStream out = new FileOutputStream(new File(filePath));
        final byte[] bytes = new byte[8192];  // FIXME(ckatsak): looks ugly
        int read = 0;
        while ((read = uploadedInputStream.read(bytes)) != -1) {
            //logger.info("Just read " + read + " bytes from the file being uploaded.");
            out.write(bytes, 0, read);
        }
        out.flush();
        out.close();
    }

}

