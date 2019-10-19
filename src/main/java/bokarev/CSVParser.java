package bokarev;
import org.apache.hadoop.io.Text;

public class CSVParser {
    public static String[] parseFlights(Text s) {
        return s.toString().split(",");
    }
    public static String[] parseAirports (Text s) {
        return s.toString().split("\",\"");
    }
    public static String getAirportName (String s) {
        return s.replaceAll("[\"]","");
    }
}