package bokarev;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] pieces = CSVParser.parseAirports(value);
        if (!CSVParser.getAirCode(pieces).contains("Code")) {
            String airportName = CSVParser.getAirportName(pieces);
            String airportCode = CSVParser.getAirCode(pieces);

            context.write(new TextPair(CSVParser.removeQuotes(airportCode), "0"), new Text(CSVParser.removeQuotes(airportName)));
        }
    }
}