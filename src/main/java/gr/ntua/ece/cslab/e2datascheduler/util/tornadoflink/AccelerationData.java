package gr.ntua.ece.cslab.e2datascheduler.util.tornadoflink;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.ArrayList;


/**
 *
 */
public class AccelerationData implements Serializable {

    private byte[] rawData;
    private int inputSize;
    private int returnSize;
    private GroupByData groupByData;
    private TypeInformation typeInfo;
    private boolean broadcasted;
    private boolean differentReturnTupleFields;
    private ArrayList<Integer> returnFieldSizes;
    // array field
    private boolean arrayField;
    private int lengthOfArrayField;
    private int arrayFieldNo;
    private int totalBytes;
    private int recordSize;

    public AccelerationData () { }

    public AccelerationData(byte[] rawData, int inputSize) {
        this.rawData = rawData;
        this.inputSize = inputSize;
    }

    public AccelerationData(byte[] rawData, int inputSize, int returnSize) {
        this.rawData = rawData;
        this.inputSize = inputSize;
        this.returnSize = returnSize;
    }

    public AccelerationData(byte[] rawData, int inputSize, GroupByData groupByData) {
        this.rawData = rawData;
        this.inputSize = inputSize;
        this.groupByData = groupByData;
    }

    public AccelerationData(byte[] rawData, int inputSize, int returnSize, GroupByData groupByData) {
        this.rawData = rawData;
        this.inputSize = inputSize;
        this.returnSize = returnSize;
        this.groupByData = groupByData;
    }

    public AccelerationData(byte[] rawData, int inputSize, TypeInformation typeInfo, boolean broadcasted) {
        this.rawData = rawData;
        this.inputSize = inputSize;
        this.typeInfo = typeInfo;
        this.broadcasted = broadcasted;
    }

    public void setRawData (byte[] rawData) {
        this.rawData = rawData;
    }

    public byte[] getRawData() {
        return rawData;
    }

    public void setInputSize (int inputSize) {
        this.inputSize = inputSize;
    }

    public int getInputSize() {
        return inputSize;
    }

    public void setReturnSize (int returnSize) {
        this.returnSize = returnSize;
    }

    public int getReturnSize() {
        return returnSize;
    }

    public GroupByData getGroupByData() {
        return groupByData;
    }

    public TypeInformation getTypeInfo() {
        return typeInfo;
    }

    public void setBroadcasted (boolean broadcasted) {
        this.broadcasted = broadcasted;
    }

    public boolean isBroadcasted () {
        return this.broadcasted;
    }

    public boolean isDifferentReturnTupleFields() {
        return differentReturnTupleFields;
    }

    public void setDifferentReturnTupleFields (boolean differentTupleFields) {
        this.differentReturnTupleFields = differentTupleFields;
    }

    public ArrayList<Integer> getReturnFieldSizes () {
        return this.returnFieldSizes;
    }

    public void setReturnFieldSizes (ArrayList<Integer> fieldSizes) {
        this.returnFieldSizes = fieldSizes;
    }

    public void hasArrayField () {
        this.arrayField = true;
    }

    public boolean getArrayField() {
        return this.arrayField;
    }

    public void setLengthOfArrayField (int lengthOfArrayField) {
        this.lengthOfArrayField = lengthOfArrayField;
    }

    public int getLengthOfArrayField() {
        return this.lengthOfArrayField;
    }

    public void setArrayFieldNo (int arrayFieldNo) {
        this.arrayFieldNo = arrayFieldNo;
    }

    public int getArrayFieldNo () {
        return this.arrayFieldNo;
    }

    public void setTotalBytes (int totalBytes) {
        this.totalBytes = totalBytes;
    }

    public int getTotalBytes () {
        return this.totalBytes;
    }

    public void setRecordSize (int recordSize) {
        this.recordSize = recordSize;
    }

    public int getRecordSize () {
        return this.recordSize;
    }

}
