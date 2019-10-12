import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class FlightComparator implements WritableComparator<>{
    public FlightComparator() {

    }

    @Override
    public int compare(TextPair a, TextPair b) {
        return a.first.compareTo(b.first);
    }
}
