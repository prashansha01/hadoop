package mapreduce.demo.problems.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("Indexing");
        job.setJarByClass(InvertedIndex.class);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");

        job.setMapperClass(MapperForInvertedIndex.class);
        job.setReducerClass(ReducerForInvertedIndex.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path input = new Path("/home/tcs/hadoop-git/hadoop-mapreduce/hadoop/src");
        Path output = new Path("/home/tcs/hadoop-git/hadoop-mapreduce/hadoop/data/output/invertedIndex");

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        FileInputFormat.setInputDirRecursive(job, true);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new InvertedIndex(), args);
        System.exit(exitCode);
    }
}
