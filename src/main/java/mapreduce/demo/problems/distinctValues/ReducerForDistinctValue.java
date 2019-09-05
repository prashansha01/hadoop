package mapreduce.demo.problems.distinctValues;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerForDistinctValue extends Reducer<Text, IntWritable, NullWritable, Text>{

    public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), key);
    }
}
