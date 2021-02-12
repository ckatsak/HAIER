package gr.ntua.ece.cslab.e2datascheduler.ml.impl;

import gr.ntua.ece.cslab.e2datascheduler.beans.cluster.HwResource;
import gr.ntua.ece.cslab.e2datascheduler.beans.ml.InferenceServiceRequest;
import gr.ntua.ece.cslab.e2datascheduler.beans.ml.InferenceSvcRequest;
import gr.ntua.ece.cslab.e2datascheduler.graph.ScheduledJobVertex;
import gr.ntua.ece.cslab.e2datascheduler.ml.Model;
import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.TornadoFeatureVector;
import gr.ntua.ece.cslab.e2datascheduler.util.HaierCacheException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;


public class EvalModel extends Model {

    private static final Logger logger = Logger.getLogger(EvalModel.class.getCanonicalName());

    public static final ResourceBundle resourceBundle = ResourceBundle.getBundle("config");
    private static final String INFERENCE_SVC_URL = resourceBundle.getString("inference.svc.url");

    private final PredictionCache execTimePredictionCache = new PredictionCache();
    private final PredictionCache powerConsPredictionCache = new PredictionCache();

    /**
     * Loads a model from disk.
     *
     * @param path
     */
    @Override
    public void load(final String path) {
        logger.finest(EvalModel.class.getCanonicalName() + ".load(\"" + path + "\") was called...");
    }

    /**
     * Make a prediction of the value of the given {@code objective} if the provided
     * {@link ScheduledJobVertex} is executed on the given {@code device} (i.e., {@link HwResource}).
     *
     * @param objective          The objective to make the prediction for
     * @param device             The {@link HwResource} allocated for the underlying {@link JobVertex}
     * @param scheduledJobVertex The {@link ScheduledJobVertex} that represents the {@link JobVertex} to be executed
     * @return The predicted value for the provided objective
     */
    @Override
    public double predict(
            final String objective,
            final HwResource device,
            final ScheduledJobVertex scheduledJobVertex
    ) {
        if (null == device) {
            throw new IllegalArgumentException("Parameter 'device' cannot be null");
        }
        if (null == scheduledJobVertex) {
            throw new IllegalArgumentException("Parameter 'scheduledJobVertex' cannot be null");
        }
        switch (objective) {
            case "execTime":
                return this.predictObjective(objective, this.execTimePredictionCache, device, scheduledJobVertex);
            case "powerCons":
                return this.predictObjective(objective, this.powerConsPredictionCache, device, scheduledJobVertex);
            default:
                logger.severe("Unknown objective '" + objective + "'; panicking...");
                throw new RuntimeException("Unknown objective '" + objective + "'");
        }
    }

    /**
     *
     * @param objective
     * @param predictionCache
     * @param device
     * @param scheduledJobVertex
     * @return
     */
    private double predictObjective(
            final String objective,
            final PredictionCache predictionCache,
            final HwResource device,
            final ScheduledJobVertex scheduledJobVertex
    ) {
        double ret;

        try {
            ret = predictionCache.getPrediction(scheduledJobVertex.getJobVertex(), device);
        } catch (final HaierCacheException e) {
            final double upstreamResponse = inferenceFromNetwork(
                    objective,
                    device,
                    scheduledJobVertex.getTornadoFeatures()
            );
            predictionCache.update(
                    scheduledJobVertex.getJobVertex(),
                    device,
                    upstreamResponse
            );

            // NOTE(ckatsak): If it fails again, there must be some kind of bug around the PredictionCache logic.
            try {
                ret = predictionCache.getPrediction(scheduledJobVertex.getJobVertex(), device);
                if (ret != upstreamResponse) {
                    throw new HaierCacheException("ret != modelResponse");
                }
            } catch (final HaierCacheException nested) {
                logger.log(Level.SEVERE, "PredictionCache appears to be bugged: " + nested.getMessage(), nested);
                throw nested;
            }
        }

        return ret;
    }

    /**
     *
     * @param objective
     * @param device
     * @param tornadoFeatures
     * @return
     */
    private double inferenceFromNetwork(
            final String objective,
            final HwResource device,
            final List<TornadoFeatureVector> tornadoFeatures) {
        final double[] upstreamResponses = new double[tornadoFeatures.size()];
        for (int i = 0; i < tornadoFeatures.size(); i++) {
            upstreamResponses[i] = EvalModel.queryModel(objective, device, tornadoFeatures.get(i));
        }
        return this.combinePredictedValues(upstreamResponses);
    }

    /**
     *
     * @param upstreamResponses
     * @return
     */
    private double combinePredictedValues(final double[] upstreamResponses) {
        return Arrays.stream(upstreamResponses).sum();
    }

    /**
     *
     * @param objective
     * @param device
     * @param tornadoFeatures
     * @return
     */
    private static double queryModel(
            final String objective,
            final HwResource device,
            final TornadoFeatureVector tornadoFeatures) {
        // Create a new inference service request...
        final InferenceSvcRequest inferenceServiceRequest = new InferenceSvcRequest();
        inferenceServiceRequest.setObjective(objective);
        inferenceServiceRequest.setDevice(device.getName());
        inferenceServiceRequest.setKernel(tornadoFeatures.getKernelID());
        // ...and marshall it into a JSON string...
        final ObjectMapper objectMapper = new ObjectMapper();
        final String jsonString;
        try {
            jsonString = objectMapper.writeValueAsString(inferenceServiceRequest);
        } catch (JsonProcessingException e) {
            logger.log(Level.SEVERE, "Could not JSON-marshall '" + inferenceServiceRequest + "': " + e.getMessage(), e);
            throw new RuntimeException(e);
        }

        // Send an HTTP POST request
        final HttpURLConnection httpCon;
        try {
            // Create a URL object for the ML inference service...
            final URL url = new URL(INFERENCE_SVC_URL);
            // ...open a new HTTP connection to it...
            httpCon = (HttpURLConnection) url.openConnection();
            // ...and set request's method to POST.
            httpCon.setRequestMethod("POST");
        } catch (final MalformedURLException e) {
            logger.log(Level.SEVERE, "Could not create URL for '" + INFERENCE_SVC_URL + "': " + e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (final ProtocolException e) {
            logger.log(Level.SEVERE, "Could not set HTTP request's method to 'POST': " + e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (final IOException e) {
            logger.log(Level.SEVERE, "Could not connect to '" + INFERENCE_SVC_URL + "': " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
        // Allow using the connection as an output stream...
        httpCon.setDoOutput(true);
        // ...set the Content-Type header...
        httpCon.setRequestProperty("Content-Type", "application/json; utf-8");
        // ...set the Accept header...
        httpCon.setRequestProperty("Accept", "application/json");
        // ...and write the JSON object down the stream.
        try (final OutputStream os = httpCon.getOutputStream()) {
            final byte[] jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);
            os.write(jsonBytes, 0, jsonBytes.length);
            os.flush();
        } catch (final IOException e) {
            logger.log(Level.SEVERE, "Could not write to HTTP connection's OutputStream: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }

        // Now read the HTTP response...
        final StringBuilder response = new StringBuilder();
        try(final BufferedReader br = new BufferedReader(
                new InputStreamReader(httpCon.getInputStream(), StandardCharsets.UTF_8))) {
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
        } catch (final UnsupportedEncodingException e) {
            logger.log(Level.SEVERE, "Unsupported encoding on HTTP connection's InputStream: " + e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (final IOException e) {
            logger.log(Level.SEVERE, "Could not read HTTP response from the InputStream: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
        // ...deserialize it...
        final Double predictedValue;
        try {
            predictedValue = objectMapper.readValue(response.toString(), Double.class);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not JSON-unmarshall '" + response.toString() + "': " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
        // ...and return it.
        return predictedValue;
    }

}
