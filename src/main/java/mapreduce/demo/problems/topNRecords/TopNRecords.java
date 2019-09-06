package mapreduce.demo.problems.topNRecords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNRecords extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TopNRecords.class);
        job.setJobName("Find out top 10 popular movies");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(MapperForTopNRecords.class);
        job.setReducerClass(ReducerForTopNRecords.class);

        Path input = new Path(strings[0]);
        Path output = new Path(strings[1]);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[]args) throws Exception {
        int exitCode = ToolRunner.run(new TopNRecords(), args);
        System.exit(exitCode);
    }
}
