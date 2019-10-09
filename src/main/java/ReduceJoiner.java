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
        String timeDelay = "";
        for (Text value:values) {
            String pieces[] = value.toString().split(";");
            if (pieces[0].equals("airportName")) {
                airportName = pieces[1];
            }
            else if (pieces[0].equals("delayTime")) {
                    timeDelay = pieces[1];
            }
            context.write(new Text(airportName), new Text(timeDelay));
        }
    }
}