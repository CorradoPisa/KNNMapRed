package it.cnr.isti.pad;

import org.apache.hadoop.io.WritableComparable;
import java.io.*;

public class Digit implements WritableComparable<Digit>
{
    private Double distance;
    private Integer prediction;
    private Integer actualValue;

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
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
        this.distance = in.readDouble();
        this.prediction = in.readInt();
        this.actualValue = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(distance);
        out.writeInt(prediction);
        out.writeInt(actualValue);
    }

    @Override
    public int compareTo(Digit other) {
        return this.distance.compareTo(other.distance);
    }
}