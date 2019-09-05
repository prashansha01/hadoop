package mapreduce.demo.problems.dataFilter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperForDataFilter extends Mapper<LongWritable, Text, NullWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = value.toString().split(" ");
        Double amount = Double.parseDouble(input[3]);
        if(amount>500.0d){
            context.write(NullWritable.get(),value);
        }
    }
}
