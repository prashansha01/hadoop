package mapreduce.demo.problems.youtubeAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class ReducerForTop5Category extends Reducer<Text, IntWritable, Text, LongWritable> {

    private PriorityQueue<Category> categories;
    private int n = 5;
    private Category data;

    public void setup(Context context){
        categories = new PriorityQueue<Category>();
        data = new Category();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //System.out.println("Insider reducer with key: "+key.toString());
        Long total = 0L;
        for(IntWritable value: values){
            total = total+ value.get();
        }
        //Category data = new Category(new LongWritable(total), key);
        data.setCategory(key);
        data.setTotalCount(new LongWritable(total));
        System.out.println("Current data is: "+data.toString());
        categories.add(data);

        System.out.println("Added data to queue: "+ data.toString());
        System.out.println("Printing the queue: "+categories.toString());
        /*if(categories.size()<n || data.compareTo(categories.peek())>0){
            categories.add(data);

            System.out.println("Added data to queue: "+ data.toString());
            System.out.println("Printing the queue: "+categories.toString());
            System.out.println("Size of the queue is : "+categories.size());
            if(categories.size()>n){
                categories.poll();
                System.out.println("Element removed");
            }
            System.out.println("Size of the queue is : "+categories.size());
        }*/
        //System.out.println("Inside reduce. Printing the queue: "+categories.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Inside Cleanup. Printing the queue: "+categories.toString());
        for (Category category: categories ){
            context.write(category.getCategory(), category.getTotalCount());
        }
    }
}
