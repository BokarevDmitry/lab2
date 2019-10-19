package bokarev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair>{
    public  Text code;
    public  Text second;

    public TextPair(Text code, Text second) {
        this.code = code;
        this.second = second;
    }
    public TextPair(String code, String second){
        this.code =new Text(code);
        this.second=new Text(second);
    }

    public Text getCode() {
        return code;
    }

    public Text getSecond() {
        return second;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPair textPair = (TextPair) o;
        return Objects.equals(code, textPair.code) &&
                Objects.equals(second, textPair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, second);
    }

    @Override
    public String toString() {
        return code +"\t"+second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        code.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        code.write(out);
        second.write(out);
    }


    @Override
    public int compareTo(TextPair tp) {
        int cmp= code.compareTo(tp.getCode());
        if(cmp!=0)
            return cmp;
        return second.compareTo(tp.getSecond());

    }
}