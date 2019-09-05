package mapreduce.demo.problems.dataFilter;

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

public class DataFilter extends Configured implements Tool{
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataFilter.class);
        job.setJobName("Filter out records from Orders dataset");

        job.setMapperClass(MapperForDataFilter.class);
        job.setReducerClass(ReducerForDataFilter.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        int status = job.waitForCompletion(true)?0:1;
        return status;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DataFilter(), args);
        System.exit(exitCode);
    }
}
