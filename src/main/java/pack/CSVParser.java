package pack;

import org.apache.hadoop.io.Text;

public class CSVParser {
    public static String[] parseCSV (Text s) {
        return s.toString().split(",");
    }

    public static String getAirportName (String s) {
        return s.replaceAll("[\"]","");
    }
}
