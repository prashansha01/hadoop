package mapreduce.demo.problems.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>{


    public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        System.out.println("======================Inside Reducer=================");
        int total = 0;
        for(IntWritable count: counts){
            total += count.get();

        }
        context.write(key, new IntWritable(total));
    }
}
