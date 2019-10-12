import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Iterator;

public class ReduceJoiner extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Double time = 0.00;
        Double minTime = Double.MAX_VALUE;
        Double maxTime = 0.00;

        Iterator<Text> iter = values.iterator();
        Text airportName = new Text(iter.next());
        System.out.println("airport Name - " + airportName);
        while (iter.hasNext()) {
            String timeDelayInfo = iter.next().toString();
            System.out.println(timeDelayInfo);
            double timeDelay = Float.parseFloat(timeDelayInfo);

            count++;
            time += timeDelay;
            if (timeDelay<minTime) minTime=timeDelay;
            if (timeDelay>maxTime) maxTime=timeDelay;
        }
        if (count>0) {
            String delayLine = "Average = " + Math.round(time/count) + " Min = " + minTime + " Max = " + maxTime;
            context.write(airportName, new Text(delayLine));
        }
    }
}