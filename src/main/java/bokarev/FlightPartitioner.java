package bokarev;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class FlightPartitioner extends Partitioner<TextPair,Text> {
    @Override
    public int getPartition(TextPair key, Text value, int numReduceTasks) {
        return Integer.parseInt(key.getCode().toString()) % numReduceTasks;
    }
}
