package bokarev;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] pieces = CSVParser.parseFlights(value);
        if (!CSVParser.getDelayTime(pieces).equals("\"ARR_DELAY_NEW\"")) {
            if (CSVParser.getDelayTime(pieces).length()>0 && Float.parseFloat(CSVParser.getDelayTime(pieces))>0) {
                context.write(new TextPair(CSVParser.getAirportCode(pieces), "1"), new Text(CSVParser.getDelayTime(pieces)));
            }
        }
    }
}