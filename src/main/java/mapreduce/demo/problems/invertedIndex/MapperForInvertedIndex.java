package mapreduce.demo.problems.invertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperForInvertedIndex extends Mapper<LongWritable, Text, Text, Text> {



    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
        The input key and value of this file is line number and the value against it in a particular file
         */
        //get the current file name from context
        FileSplit filesplit = (FileSplit) context.getInputSplit();
        final String fileName = filesplit.getPath().getName();
        System.out.println("Reading file: "+ fileName);

        // line no 1, text - This is line number one
        //get each word and write it as a key of the mapper output, the value remains contant for all the keys i.e the filename
        //Output - [{This, filename}, {is, filename}, {line, filename}, {number, filename}, {one, filename}]

        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        Text word = new Text();
        while (tokenizer.hasMoreTokens()){
            word.set(new Text(tokenizer.nextToken()));
            context.write(word, new Text(fileName));
        }
    }


}
