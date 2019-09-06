package mapreduce.demo.problems.topNRecords;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class ReducerForTopNRecords extends Reducer<Text, LongWritable, Text, LongWritable> {

    private TreeMap<Long, String> treemap;

    public void setup(Context context){
        treemap = new TreeMap<Long, String>(new LongComparator());
    }

    public void reduce(Text key, Iterable<LongWritable> value, Context context){
        System.out.println("======================Inside Reducer==================");
        Long count = 0L;

        for(LongWritable c: value){

            count= count+c.get();
        }

        treemap.put(count, key.toString());

        if(treemap.size()>10){
            treemap.remove(treemap.firstKey());
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Long, String> entry: treemap.entrySet()){
            String moviename= entry.getValue();
            Long count = entry.getKey();

            context.write(new Text(moviename), new LongWritable(count));
        }

    }

}
