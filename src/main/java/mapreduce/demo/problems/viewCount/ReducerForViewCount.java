package mapreduce.demo.problems.viewCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerForViewCount extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("======================Inside Reducer=================");
        int count = 0;
        for(IntWritable value: values){
            count+= value.get();
        }
        context.write(key, new IntWritable(count));
    }
}

