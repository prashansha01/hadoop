package mapreduce.demo.problems.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class MapperForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        System.out.println("Reading value from file: "+values.toString());
        String[] words = values.toString().split(";");
        for(int i =0; i<words.length; i++){
            context.write(new Text(words[i]), new IntWritable(1));
        }
    }
}
