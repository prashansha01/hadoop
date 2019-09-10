package mapreduce.demo.problems.topnUsingPriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

public class MapperForTopN extends Mapper<LongWritable, Text, NullWritable, Text> {
    private PriorityQueue<Movie> moviesPriority;
    private int n;

    public void setup(Context context){
        System.out.println("Inside setup");
        moviesPriority = new PriorityQueue<Movie>();
        n=5;
    }

    public void map(LongWritable key, Text value, Context context){

        System.out.println("==============Inside Mapper====================");
        String[] data = value.toString().split("\t");
        System.out.println("==============Got data "+value.toString());
        System.out.println("==============spliting data, got size "+data.length);
        for(String str : data){
            System.out.println(str);
            System.out.println("moviesPriority " + moviesPriority );

        }
        System.out.println("Size of queue " + moviesPriority.size() );
        int views = Integer.parseInt(data[1]);
        Movie movie = new Movie(views,value.toString());
        System.out.println("After//////////" );

        if(moviesPriority.size()< n || movie.compareTo(moviesPriority.peek())>0){
            if(moviesPriority.size()>=n){
                System.out.println("Size of queue is "+ moviesPriority.size()+" Removing first element");

                moviesPriority.poll();
            }
            moviesPriority.add(movie);
            System.out.println("Elemeent added to queue");

        }


    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        while (!moviesPriority.isEmpty()){
            context.write(NullWritable.get(), new Text(moviesPriority.poll().getData()));
        }
        System.out.println("==============Written Mapper output====================");
    }
}
