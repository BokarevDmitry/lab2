import org.apache.hadoop.io.Text;

public class CSVParcer {
    CSVParcer() {

    }

    public String[] getParced (Text s) {
        String[] pieces = s.toString().split(",");
    }
}
