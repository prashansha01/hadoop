package mapreduce.demo.problems.youtubeAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class ReducerForTop5Category extends Reducer<Text, IntWritable, Text, LongWritable> {



    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Long total = 0L;
        for(IntWritable value: values){
            total = total+ value.get();
        }
        context.write(key, new LongWritable(total));
    }
}
