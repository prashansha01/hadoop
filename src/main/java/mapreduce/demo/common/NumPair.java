package mapreduce.demo.common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumPair implements WritableComparable<NumPair> {


    private DoubleWritable sum;
    private LongWritable count;

    public NumPair(DoubleWritable sum, LongWritable count) {
        this.sum = sum;
        this.count = count;
    }
    public NumPair(Double sum, Long count) {
        this.sum = new DoubleWritable(sum);
        this.count = new LongWritable(count);
    }

    public NumPair(){
        this.sum = new DoubleWritable();
        this.count = new LongWritable();
    }

    public int compareTo(NumPair o) {

        int cmp = sum.compareTo(o.sum);
        if(cmp!=0){
            return cmp;
        }
        return count.compareTo(o.count);
    }

    public void write(DataOutput dataOutput) throws IOException {
        sum.write(dataOutput);
        count.write(dataOutput);

    }

    public void readFields(DataInput dataInput) throws IOException {
        sum.readFields(dataInput);
        count.readFields(dataInput);
    }
    @Override
    public int hashCode(){
        return sum.hashCode()*163+ count.hashCode();
    }
    @Override
    public boolean equals(Object o){
        if(o instanceof NumPair){
            NumPair n = (NumPair)o;
            return sum.equals(n.sum)&&count.equals(n.count);
        }
        return false;
    }
    public DoubleWritable getSum() {
        return sum;
    }

    public void setSum(DoubleWritable sum) {
        this.sum = sum;
    }

    public LongWritable getCount() {
        return count;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }
    @Override
    public String toString(){
        return this.getSum().get() + ", "+ this.getCount().get();
    }
}
