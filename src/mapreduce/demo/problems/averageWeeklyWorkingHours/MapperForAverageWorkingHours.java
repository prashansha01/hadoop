package mapreduce.demo.problems.averageWeeklyWorkingHours;

import mapreduce.demo.common.NumPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperForAverageWorkingHours extends Mapper<LongWritable, Text, Text, NumPair> {

    public void map(LongWritable lineNumber, Text lineData, Context context ) throws IOException, InterruptedException {
        String[] data = lineData.toString().split(", ");
        String marital_status = data[5];
        Double weeklyWorkingHours = Double.parseDouble(data[12]);
        NumPair value = new NumPair(weeklyWorkingHours, 1L);
        //System.out.println("Printing the numpair :" + value.toString());
        context.write(new Text(marital_status), value);

    }

}
