package appCalculsMeteo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeteoMapper extends Mapper<LongWritable, Text, Text, MeteoWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MeteoWritable>.Context context)
            throws IOException, InterruptedException {
        String ligne = value.toString();
        String[] vals = ligne.split("\",\"");
        if (!vals[1].equals("DATE")) {
            String tempStrVal = vals[13].substring(0, 5);
            char tempChrQuality = vals[13].substring(6, 7).charAt(0);
            long tempCount = 0;
            double temp = 0;
            System.out.println("***" + tempStrVal + "***" + tempChrQuality);
            if (!tempStrVal.equals("+9999") && Character.isDigit(tempChrQuality) && Character.getNumericValue(tempChrQuality) < 5) {
                System.out.println("Enter");
                temp = Integer.parseInt(tempStrVal) / 10.0;
                tempCount = 1;
                System.out.println("Temp = " + temp);
            }
            String speedWStrVal = vals[10].substring(8, 12);
            char speedWChrQuality = vals[10].substring(13, 14).charAt(0);
            long speedWCount = 0;
            double speedW = 0;
            System.out.println("***" + speedWStrVal + "***" + speedWChrQuality);
            if (!speedWStrVal.equals("+9999") && Character.getNumericValue(speedWChrQuality) < 4) {
                System.out.println("Enter 2");
                speedW = Integer.parseInt(speedWStrVal) / 10.0;
                speedWCount = 1;
                System.out.println("SpeedW = " + speedW);
            }
            if (speedWCount != 0 || tempCount != 0) {
                System.out.println("Enter 3");
                MeteoWritable outputValue = new MeteoWritable();
                outputValue.setNbreMesuresW(speedWCount);
                outputValue.setNbreMesuresT(tempCount);
                outputValue.setSpeedWMin(speedW);
                outputValue.setSpeedWMax(speedW);
                outputValue.setSpeedWMoy(speedW);
                outputValue.setTempMin(temp);
                outputValue.setTempMax(temp);
                String outputKey = vals[1].substring(0, 7);
                context.write(new Text(outputKey), outputValue);
                System.out.println("Map(" + outputKey + "," + outputValue + ")");
            }
        }
    }
}
