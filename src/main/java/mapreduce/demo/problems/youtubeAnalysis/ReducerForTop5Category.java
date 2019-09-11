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
    //private Category data;

    public void setup(Context context){
        System.out.println("========Setup called==========");
        categories = new PriorityQueue<Category>();
        //data = new Category();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterable<IntWritable> temp = values;
        System.out.println("Insider reducer with key: "+key.toString()+" and value: ");

        Long total = 0L;
        for(IntWritable value: values){
            total = total+ value.get();
        }
        Category data = new Category(total, key.toString());
        data.setCategory(key.toString());
        data.setTotalCount(total);
        System.out.println("Current data is: "+data.toString());

        System.out.println("Printing the queue before getting into if block: "+categories.toString());
        if(categories.size()<n || data.compareTo(categories.peek())>0){
            System.out.println("Printing the queue before adding new data: "+categories.toString());
            categories.add(data);

            System.out.println("Added data to queue: "+ data.toString());
            System.out.println("Printing the queue: "+categories.toString());
            System.out.println("Size of the queue is : "+categories.size());
            if(categories.size()>n){
                categories.poll();
                System.out.println("Element removed");
            }
            System.out.println("Size of the queue is : "+categories.size());
        }
        System.out.println("==========================================" );
    }

    private String print(Iterable<IntWritable> values) {
        int c =0;
        StringBuilder sb = new StringBuilder();
        for (IntWritable i:values) {
            sb.append(", " +i);
            c= c+1;
        }
        return "No. of values: "+ c +". Values:  "+ sb.toString();
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Inside Cleanup. Printing the queue: "+categories.toString());
        for (Category category: categories ){
            context.write(new Text(category.getCategory()), new LongWritable(category.getTotalCount()));
        }
    }
}
