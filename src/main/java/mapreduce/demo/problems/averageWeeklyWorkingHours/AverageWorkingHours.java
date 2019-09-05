package mapreduce.demo.problems.averageWeeklyWorkingHours;

import mapreduce.demo.common.NumPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageWorkingHours extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("Average weekly working hours based on marital status");
        job.setJarByClass(AverageWorkingHours.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NumPair.class);


        job.setMapperClass(MapperForAverageWorkingHours.class);
        job.setCombinerClass(CombinerForAverageWorkingHours.class);
        job.setReducerClass(ReducerForAverageWorkingsHours.class);

        job.setNumReduceTasks(1);


        Path inputpath = new Path(strings[0]);
        Path outputpath = new Path(strings[1]);
        FileInputFormat.addInputPath(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);

        int exitcode = job.waitForCompletion(true)?0:1;
        return exitcode;
    }

    public static void main (String args[]) throws Exception {
        int exitCode = ToolRunner.run(new AverageWorkingHours(), args);
        System.exit(exitCode);
    }
}
