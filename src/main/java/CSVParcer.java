import org.apache.hadoop.io.Text;

public class CSVParcer {
    public String[] parceCSV (Text s) {
        return s.toString().split(",");
    }
}
