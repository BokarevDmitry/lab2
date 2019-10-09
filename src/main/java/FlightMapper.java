import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] pieces = value.toString().split(",");
        if (pieces[18].length() > 0 && pieces[18]) {
            context.write(new Text(pieces[14]), new Text("delayTime;" + pieces[18]));
        }
    }
}