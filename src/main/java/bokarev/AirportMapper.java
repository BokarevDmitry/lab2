package bokarev;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] pieces = CSVParser.parseAirports(value);
        String airportName = CSVParser.getAirportName(pieces);
        airportName = CSVParser.removeQuotes(airportName);

        if (!airportName.contains("Code")) {
            context.write(new TextPair(CSVParser.getAirCode(pieces), "0"), new Text(airportName));
        }
    }
}