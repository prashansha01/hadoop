package mapreduce.demo.problems.topnUsingPriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopN extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("Top movies");
        job.setJarByClass(TopN.class);

        job.setMapperClass(MapperForTopN.class);
        job.setReducerClass(ReducerForTopN.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Path input = new Path(strings[0]);
        Path out = new Path(strings[1]);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new TopN(), args);
        System.exit(exit);
    }
}
