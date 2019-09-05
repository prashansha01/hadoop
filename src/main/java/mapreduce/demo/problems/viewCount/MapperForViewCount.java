package mapreduce.demo.problems.viewCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MapperForViewCount extends Mapper<LongWritable, Text, Text, IntWritable>{


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Input read from the file is" +value.toString());
        String[] row = value.toString().split(";");
        System.out.println("String array is "+row);
        System.out.println("No. of rows is:" +row.length);
        context.write(new Text(row[2]), new IntWritable(1));
    }

    /*@Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }*/
}
