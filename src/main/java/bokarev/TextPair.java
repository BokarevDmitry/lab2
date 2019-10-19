package bokarev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair>{
    public  Text first;
    public  Text second;

    public  TextPair(){
        this.first=new Text();
        this.second=new Text();
    }

    public TextPair(Text first, Text second) {
        this.first = first;
        this.second = second;
    }
    public TextPair(String first,String second){
        this.first=new Text(first);
        this.second=new Text(second);
    }

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }
    public void set(Text first,Text second){
        this.first=first;
        this.second=second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPair textPair = (TextPair) o;
        return Objects.equals(first, textPair.first) &&
                Objects.equals(second, textPair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return first+"\t"+second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }


    @Override
    public int compareTo(TextPair tp) {
        int cmp=first.compareTo(tp.getFirst());
        if(cmp!=0)
            return cmp;
        return second.compareTo(tp.getSecond());

    }

    public TextPair reverse() {
        return new TextPair(second,first);
    }
}