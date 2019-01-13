package it.cnr.isti.pad;

import org.apache.hadoop.io.WritableComparable;
import java.io.*;
import java.util.ArrayList;

public class Digit implements WritableComparable<Digit>
{
    private Double distance;
    private Integer prediction;
    private Integer actualValue;
    private ArrayList<Double> testData;

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    public ArrayList<Double> getTestData() {
        return testData;
    }

    public void setTestData(ArrayList<Double> testData) {
        this.testData = testData;
    }

    public Integer getPrediction() {
        return prediction;
    }

    public void setPrediction(Integer prediction) {
        this.prediction = prediction;
    }

    public Integer getActualValue() {
        return actualValue;
    }

    public void setActualValue(Integer actualValue) {
        this.actualValue = actualValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        testData = new ArrayList<>();
        actualValue = in.readInt();

        int numFields = in.readInt();
        if (numFields == 0) return;
        try {
            for (int i = 0; i < numFields; i++) {
                testData.add(in.readDouble());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(actualValue);

        out.writeInt(testData.size());
        if (testData.size() == 0) return;
        Double obj;
        for (int i = 0; i < testData.size(); i++) {
            obj = testData.get(i);
            if (obj == null) {
                throw new IOException("Cannot serialize null fields!");
            }
            out.writeDouble(obj);
        }
    }

    @Override
    public int compareTo(Digit other) {
        return 0;
    }
}