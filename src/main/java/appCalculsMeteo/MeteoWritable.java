package appCalculsMeteo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MeteoWritable implements Writable {
    private Long nbreMesuresW;
    private Long nbreMesuresT;
    private Double speedWMin;
    private Double speedWMax;
    private Double speedWMoy;
    private Double tempMin;
    private Double tempMax;

    public String toString() {
        return "Wind(" + nbreMesuresW + ", " + String.format("%6.2f", speedWMin) + ", " + String.format("%6.2f", speedWMax)
                + ", " + String.format("%6.2f", speedWMoy) + ")\tTemp(" + nbreMesuresT + ", " + String.format("%6.2f", tempMin) + ", " + String.format("%6.2f", tempMax) + ")";
    }

    public Long getNbreMesuresW() {
        return nbreMesuresW;
    }

    public void setNbreMesuresW(Long nbreMesuresW) {
        this.nbreMesuresW = nbreMesuresW;
    }

    public Long getNbreMesuresT() {
        return nbreMesuresT;
    }

    public void setNbreMesuresT(Long nbreMesuresT) {
        this.nbreMesuresT = nbreMesuresT;
    }

    public Double getSpeedWMin() {
        return speedWMin;
    }

    public void setSpeedWMin(Double speedWMin) {
        this.speedWMin = speedWMin;
    }

    public Double getSpeedWMax() {
        return speedWMax;
    }

    public void setSpeedWMax(Double speedWMax) {
        this.speedWMax = speedWMax;
    }

    public Double getSpeedWMoy() {
        return speedWMoy;
    }

    public void setSpeedWMoy(Double speedWMoy) {
        this.speedWMoy = speedWMoy;
    }

    public Double getTempMin() {
        return tempMin;
    }

    public void setTempMin(Double tempMin) {
        this.tempMin = tempMin;
    }

    public Double getTempMax() {
        return tempMax;
    }

    public void setTempMax(Double tempMax) {
        this.tempMax = tempMax;
    }

    public void readFields(DataInput in) throws IOException {
        nbreMesuresW = in.readLong();
        nbreMesuresT = in.readLong();
        speedWMin = in.readDouble();
        speedWMax = in.readDouble();
        speedWMoy = in.readDouble();
        tempMin = in.readDouble();
        tempMax = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(nbreMesuresW);
        out.writeLong(nbreMesuresT);
        out.writeDouble(speedWMin);
        out.writeDouble(speedWMax);
        out.writeDouble(speedWMoy);
        out.writeDouble(tempMin);
        out.writeDouble(tempMax);
    }
}
