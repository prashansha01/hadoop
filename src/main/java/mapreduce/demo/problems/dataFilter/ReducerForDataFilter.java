package mapreduce.demo.problems.dataFilter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerForDataFilter extends Reducer<NullWritable, Text, NullWritable, Text>{

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text data:values) {
            context.write(NullWritable.get(), data);
        }
    }
}
