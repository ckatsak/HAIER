package gr.ntua.ece.cslab.e2datascheduler.beans.ml;

import com.fasterxml.jackson.annotation.JsonProperty;


public class InferenceSvcRequest {

    private String objective;
    private String device;
    private String kernel;

    public InferenceSvcRequest() {}

    @JsonProperty("Objective")
    public String getObjective() {
        return this.objective;
    }

    @JsonProperty("Objective")
    public void setObjective(final String objective) {
        this.objective = objective;
    }

    @JsonProperty("Device")
    public String getDevice() {
        return this.device;
    }

    @JsonProperty("Device")
    public void setDevice(final String device) {
        this.device = device;
    }

    @JsonProperty("Kernel")
    public String getKernel() {
        return this.kernel;
    }

    @JsonProperty("Kernel")
    public void setKernel(final String kernel) {
        this.kernel = kernel;
    }

}
