import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class FlightComparator implements WritableComparator<TextPair>{
    public FlightComparator() {
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        
    }
}
