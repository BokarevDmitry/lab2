import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] pieces = CSVParcer
        if (!pieces[18].equals("\"ARR_DELAY_NEW\"")) {
            if (pieces[18].length()>0 && Float.parseFloat(pieces[18])>0) {
                context.write(new TextPair(pieces[14], "1"), new Text(pieces[18]));
            }
        }
    }
}