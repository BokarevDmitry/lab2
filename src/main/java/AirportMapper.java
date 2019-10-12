import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] pieces = value.toString().split("\",\"");
        if (!pieces[0].equals("Code,Description")) {
            context.write(new TextPair(pieces[0].substring(1), ""), new Text("airportName;" + pieces[1]));
        }
        else {
            context.write(new Text("code"), new Text("airportName;descrip"));
        }
    }
}