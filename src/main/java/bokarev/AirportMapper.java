package bokarev;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static String DESCRIPTION_LINE = "Code";

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] columns = CSVParser.parseAirports(value);
        String airportCode = CSVParser.getAirCode(columns);

        if (!airportCode.contains(DESCRIPTION_LINE)) {
            String airportName = CSVParser.getAirportName(columns);
            context.write(new TextPair(CSVParser.removeQuotes(airportCode), "0"), new Text(CSVParser.removeQuotes(airportName)));
        }
    }
}