package gr.ntua.ece.cslab.e2datascheduler.optimizer.nsga.exhaustivetimeevaluation;

import gr.ntua.ece.cslab.e2datascheduler.beans.cluster.HwResource;

import java.util.HashMap;

class Evaluation {

    private final Graph graph;
    private long currentTime;
    private final TaskList tasks;
    private final HashMap<HwResource, Long> devices;

    Evaluation(final Graph graph, final long currentTime, final TaskList tasks, final HashMap<HwResource, Long> devices) {
        this.graph = graph;
        this.currentTime = currentTime;
        this.tasks = tasks;
        this.devices = devices;
    }

    boolean isComplete() {
        for (Task task : this.graph.getSinks()) {
            if (this.tasks.get(task.getIndex()).getPathCost() == -1) {
                return false;
            }
        }
        return true;
    }

    boolean dataDepReady(final Task task) {
        for (int parent : task.getParents()) {
            long pathCost = this.tasks.get(parent).getPathCost();
            if (pathCost == -1 || pathCost > this.currentTime) {
                return false;
            }
        }
        return true;
    }

    long getCurrentTime() {
        return currentTime;
    }

    void timeStep(final long step) {
        currentTime += step;
    }

    TaskList getTasks() {
        return tasks;
    }

    HashMap<HwResource, Long> getDevices() {
        return devices;
    }

    @Override
    public String toString() {
        return "Evaluation<" +
                "currentTime=" + currentTime +
                ", tasks=" + tasks +
                ", devices=" + devices +
                '>';
    }

}
