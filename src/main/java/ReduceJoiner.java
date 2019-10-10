import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Iterator;

public class ReduceJoiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String airportName = "";
        String delayLine = "";

        int count = 0;
        Double time = 0.00;
        Double minTime = 9999.00;
        Double maxTime = 0.00;


        for (Text value:values) {
            String pieces[] = value.toString().split(";");
            if (pieces[0].equals("airportName")) {
                airportName = pieces[1];
            }
            else if (pieces[0].equals("delayTime")) {
                    //timeDelay = pieces[1];
                    count++;
                    double timeDelay = Float.parseFloat(pieces[1]);

                    time += timeDelay;
                    if (timeDelay<minTime) minTime=timeDelay;
                    if (timeDelay>maxTime) maxTime=timeDelay;
            }
        }
        if (time>0) {
            delayLine = "Total DelayTime = " + time + " Average = " + time/count + " Min = " + minTime + " Max = " + maxTime;
            context.write(new Text(airportName), new Text(delayLine));
        }
    }
}