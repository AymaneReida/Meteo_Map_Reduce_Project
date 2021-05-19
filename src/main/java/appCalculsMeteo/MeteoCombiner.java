package appCalculsMeteo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeteoCombiner extends Reducer<Text, MeteoWritable, Text, MeteoWritable> {

    @Override
    protected void reduce(Text key, Iterable<MeteoWritable> values,
                          Reducer<Text, MeteoWritable, Text, MeteoWritable>.Context context) throws IOException, InterruptedException {
        long speedWCount = 0, tempCount = 0;
        Double speedWMin = null, speedWMax = null, tempMin = null, tempMax = null;
        double speedWSum = 0;
        for (MeteoWritable val : values) {
            if (val.getNbreMesuresW() != 0) {
                speedWCount += val.getNbreMesuresW();
                if (speedWMin == null || speedWMin > val.getSpeedWMin()) {
                    speedWMin = val.getSpeedWMin();
                }
                if (speedWMax == null || speedWMax < val.getSpeedWMax()) {
                    speedWMax = val.getSpeedWMax();
                }
                speedWSum += val.getSpeedWMoy();
            }

            if (val.getNbreMesuresT() != 0) {
                tempCount += val.getNbreMesuresT();
                if (tempMin == null || tempMin > val.getTempMin()) {
                    tempMin = val.getTempMin();
                }
                if (tempMax == null || tempMax < val.getTempMax()) {
                    tempMax = val.getTempMax();
                }
            }
        }
        MeteoWritable outputValue = new MeteoWritable();
        outputValue.setNbreMesuresW(speedWCount);
        outputValue.setNbreMesuresT(tempCount);
        outputValue.setSpeedWMin(speedWMin);
        outputValue.setSpeedWMax(speedWMax);
        outputValue.setSpeedWMoy(speedWSum); // Somme partielle
        outputValue.setTempMin(tempMin);
        outputValue.setTempMax(tempMax);
        context.write(key, outputValue);
    }
}
