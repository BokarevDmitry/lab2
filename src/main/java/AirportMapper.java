import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] pieces = value.toString().split(",");
        context.write(new Text(pieces[0]), new Text("airportName;" + pieces[1]));
    }
}