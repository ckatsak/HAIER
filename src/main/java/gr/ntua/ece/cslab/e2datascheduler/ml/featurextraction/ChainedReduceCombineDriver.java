package gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction;

import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.AccelerationData;
//import gr.ntua.ece.cslab.e2datascheduler.ml.featurextraction.tornadoflink.GroupByData;

//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.DelegatingConfiguration;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * TODO(ckatsak): Documentation
 */
public class ChainedReduceCombineDriver {

    public void fakeCompile() {

//        Configuration conf = ((DelegatingConfiguration) this.config.getConfiguration()).getBackingConfig();
//        AccelerationData acdata = (AccelerationData) conf.getAccelerationData();
        AccelerationData acdata = new AccelerationData();

        byte[] tornadoData = acdata.getRawData();
        int numOfcentroids = 2; // this is fixed to two
        int returnSizeRed = acdata.getReturnSize();
        int[] numberOfElementsPerCentroid = new int[numOfcentroids];
        // how many elements correspond to each centroid

        for (int i = 0; i < tornadoData.length; i += returnSizeRed) {
            int centroid = tornadoData[i] - 1;
            numberOfElementsPerCentroid[centroid]++;
        }


        // create the byte arrays that will store the bytes of each centroid
        HashMap<Integer, ArrayList<byte[]>> bytesOfCentroids = new HashMap();

        for (int i = 0; i < numOfcentroids; i++) {
            ArrayList<byte[]> bytesOfData = new ArrayList<>();
            byte[] idBytes = new byte[8 * numberOfElementsPerCentroid[i]];
            byte[] pointxBytes = new byte[8 * numberOfElementsPerCentroid[i]];
            byte[] pointyBytes = new byte[8 * numberOfElementsPerCentroid[i]];
            byte[] counterBytes = new byte[8 * numberOfElementsPerCentroid[i]];
            bytesOfData.add(idBytes);
            bytesOfData.add(pointxBytes);
            bytesOfData.add(pointyBytes);
            bytesOfData.add(counterBytes);
            bytesOfCentroids.put(i, bytesOfData);
        }

        for (int c = 0; c < numOfcentroids; c++) {
            int t = 0;
            int numId = 0;
            int numPointx = 0;
            int numPointy = 0;
            int numCount = 0;
            for (int k = t; k < tornadoData.length; k += returnSizeRed) {
                if (tornadoData[k] == c + 1) {
                    // id
                    for (int b = 0; b < 8; b++) {
                        bytesOfCentroids.get(c).get(0)[b + numId] = tornadoData[k + b];
                    }
                    numId += 8;
                    // pointx
                    for (int b = 0; b < 8; b++) {
                        bytesOfCentroids.get(c).get(1)[b + numPointx] = tornadoData[k + b + 8];
                    }
                    numPointx += 8;
                    // pointy
                    for (int b = 0; b < 8; b++) {
                        bytesOfCentroids.get(c).get(2)[b + numPointy] = tornadoData[k + b + 16];
                    }
                    numPointy += 8;
                    // counter
                    for (int b = 0; b < 8; b++) {
                        bytesOfCentroids.get(c).get(3)[b + numCount] = tornadoData[k + b + 24];
                    }
                    numCount += 8;
                }
                t += returnSizeRed;
            }
        }
//        //		long groupByEnd = System.currentTimeMillis();
////						System.out.println("### GroupBy " + (groupByEnd - groupByBegin));
////
////						for (int i = 0; i < bytesOfCentroids.size(); i++) {
////							System.out.println("== GroupBy: Centroid " + (i + 1));
////							for (int j = 0; j < bytesOfCentroids.get(i).get(0).length; j++) {
////								System.out.print(bytesOfCentroids.get(i).get(0)[j] + " ");
////							}
////							System.out.println();
////							for (int j = 0; j < bytesOfCentroids.get(i).get(1).length; j++) {
////								System.out.print(bytesOfCentroids.get(i).get(1)[j] + " ");
////							}
////							System.out.println();
////							for (int j = 0; j < bytesOfCentroids.get(i).get(2).length; j++) {
////								System.out.print(bytesOfCentroids.get(i).get(2)[j] + " ");
////							}
////							System.out.println();
////							for (int j = 0; j < bytesOfCentroids.get(i).get(3).length; j++) {
////								System.out.print(bytesOfCentroids.get(i).get(3)[j] + " ");
////							}
////							System.out.println();
////						}

//        GroupByData gb = new GroupByData(numberOfElementsPerCentroid, bytesOfCentroids);
//        AccelerationData tdata = new AccelerationData(acdata.getRawData(), acdata.getInputSize(), acdata.getReturnSize(), gb);
//        conf.setAccelerationData(tdata);
//        outputCollector.collect(null);
//        return;
    }

}
