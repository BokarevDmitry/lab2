import org.apache.hadoop.io.Text;

public class CSVParcer {
    public String[] getParced (Text s) {
        return s.toString().split(",");
    }
}
