package mapreduce.demo.problems.distinctValues;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperForDistinctValue extends Mapper <LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] words = line.toString().split(" ");
        for(int i =0; i<words.length; i++){
            context.write(new Text(words[i].replaceAll("[-+.^:,?!]","")), new IntWritable(1));
        }
    }
}
