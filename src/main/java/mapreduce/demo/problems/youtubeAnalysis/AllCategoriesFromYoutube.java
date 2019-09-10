package mapreduce.demo.problems.youtubeAnalysis;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AllCategoriesFromYoutube extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        // uncomment this for all categories
        //job.setJobName("All categories from Youtube data");
        job.setJobName("Top n categories from Youtube data");
        job.setJarByClass(AllCategoriesFromYoutube.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(MapperForAllCategoriesFromYoutube.class);
        // uncomment this for all categories
         //job.setReducerClass(ReducerForAllCategoriesFromYoutube.class);
        job.setReducerClass(ReducerForTop5Category.class);

        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true)?0:1;

    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AllCategoriesFromYoutube(), args);
        System.exit(exitCode);
    }

}
