package bokarev;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class FlightComparator extends WritableComparator{
    public FlightComparator() {
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextPair a1 = (TextPair) a;
        TextPair b1 = (TextPair) b;
        return a1.first.compareTo(b1.first);
    }
}
