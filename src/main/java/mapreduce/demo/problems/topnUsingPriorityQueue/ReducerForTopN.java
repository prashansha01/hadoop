package mapreduce.demo.problems.topnUsingPriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.awt.peer.TextAreaPeer;
import java.io.IOException;
import java.util.PriorityQueue;

public class ReducerForTopN extends Reducer<NullWritable, Text, NullWritable, Text> {
    private PriorityQueue<Movie> moviesPriority;
    private int n= 5;
    public void setup(Context context){
         moviesPriority= new PriorityQueue<Movie>();
    }

    public void reduce(NullWritable key, Iterable<Text> values, Context context){
        System.out.println("==============Inside Reducer====================");
        for(Text value: values) {
            String[] data = value.toString().split("\t");
            int views = Integer.parseInt(data[1]);
            Movie movie = new Movie(views, value.toString());

            if (moviesPriority.size() < n || movie.compareTo(moviesPriority.peek()) > 0) {
                if (moviesPriority.size() >= n) {
                    moviesPriority.poll();
                }
                moviesPriority.add(movie);
            }
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        while (!moviesPriority.isEmpty()){
            context.write(NullWritable.get(), new Text(moviesPriority.poll().getData()));
        }
        System.out.println("==============Written Reducer output====================");
    }
}
