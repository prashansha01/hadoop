package mapreduce.demo.problems.topNRecords;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MapperForTopNRecords extends Mapper<LongWritable, Text, Text, LongWritable> {

    private TreeMap<Long, String> treemap;
    public void setup(Context context){
        treemap = new TreeMap<Long, String>();
    }

    public void map(LongWritable key, Text line, Context context){
        System.out.println("======================Inside Mapper==================");
        String[] data = line.toString().split("\t");
        String movieName = data[0];
        Long noOfViews = Long.parseLong(data[1]);
        treemap.put(noOfViews, movieName);
        System.out.println("Size of map: "+treemap.size());
        if(treemap.size()>10){
            System.out.println("Inside If condition, removing first key");
            System.out.println("Before removing: "+treemap.toString());
            System.out.println("Printing key: "+treemap.firstKey()+" "+ treemap.get(treemap.firstKey()));
            treemap.remove(treemap.firstKey());
            System.out.println("After removing: "+treemap.toString());
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Long, String> entry: treemap.entrySet()){
            context.write(new Text(entry.getValue()), new LongWritable(entry.getKey()));
        }
    }

}
