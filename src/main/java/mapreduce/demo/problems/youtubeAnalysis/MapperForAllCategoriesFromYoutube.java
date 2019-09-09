package mapreduce.demo.problems.youtubeAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperForAllCategoriesFromYoutube extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //read through the line and gwet the category
        // write to context, key- category, value - one (Write each categoty with count of one into the context)
        System.out.println("Line:  " + value.toString());
        String[] data = value.toString().split("\t");
        if(data.length>3) {
            Text category = new Text(data[3]);
            context.write(category, new IntWritable(1));
        }


    }
}
