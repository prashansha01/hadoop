import mapreduce.demo.problems.youtubeAnalysis.Category;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;

public class PriorityQueueDemo {

    private Category cat1 = new Category(new LongWritable(1l), new Text("One"));

    public static void main(String[] args){
        Category cat1 = new Category(new LongWritable(10l), new Text("Ten"));
        Category cat2 = new Category(new LongWritable(2l), new Text("Three"));
        Category cat3 = new Category(new LongWritable(8l), new Text("Eight"));
        Category cat4 = new Category(new LongWritable(7l), new Text("Seven"));
        Category cat5 = new Category(new LongWritable(4l), new Text("Four"));
        Category cat6 = new Category(new LongWritable(11l), new Text("Eleven"));
        List<Category> list = new ArrayList<Category>();
        list.add(cat1);list.add(cat2);
        list.add(cat3);list.add(cat4);
        list.add(cat5);list.add(cat6);

        PriorityQueue<Category> pq = new PriorityQueue<Category>();
        for(Category cat: list){
            if(pq.size()<3 || cat.compareTo(pq.peek())>0){
                pq.add(cat);

                System.out.println("Added data to queue: "+ cat.toString());
                System.out.println("Printing the queue: "+pq.toString());
                System.out.println("Size of the queue is : "+pq.size());
                if(pq.size()>3){
                    pq.poll();
                    System.out.println("Element removed");
                }
                System.out.println("Size of the queue is : "+pq.size());
            }
        }
        System.out.println("List of elemenets :");
        System.out.println(list.toString());
        System.out.println("Top 3 elemenets :");
        System.out.println(pq.toString());

    }
}
