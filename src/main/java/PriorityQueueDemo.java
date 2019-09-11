import mapreduce.demo.problems.youtubeAnalysis.Category;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.*;

public class PriorityQueueDemo {



    public static void main(String[] args){
        Category cat1 = new Category(10l, "Ten");
        Category cat2 = new Category(2l, "Three");
        Category cat3 = new Category( 8l, "Eight");
        Category cat4 = new Category( 7l, "Seven");
        Category cat5 = new Category( 4l,  "Four");
        Category cat6 = new Category( 11l,  "Eleven");
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
