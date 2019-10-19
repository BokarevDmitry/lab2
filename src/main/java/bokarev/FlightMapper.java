package bokarev;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static String DESCRIPTION_LINE = "ARR_DELAY_NEW";

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] columns = CSVParser.parseFlights(value);
        String delayTime = CSVParser.getDelayTime(columns);

        if (!delayTime.contains(DESCRIPTION_LINE) && delayTime.length()>0 && Float.parseFloat(delayTime)>0) {
            context.write(new TextPair(CSVParser.getAirportCode(columns), "1"), new Text(delayTime));
        }
    }
}