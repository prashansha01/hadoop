package mapreduce.demo.problems.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReducerForInvertedIndex extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*

        the key here is one unique word that is available in the file system, the value is an iterable of file names where the word occurs

        Reducer output:
        key - key received i.e key of the mapper output which the framework sorts in such a way there is one key for each unique word with the values as
        the list of filenames
        We iterate over the list of filenames and write to the reducer output as a pipe separated string
        We need to set the separator details in the job configuration

         */

        StringBuilder listOfFileNames = new StringBuilder();
        final Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            listOfFileNames.append(iterator.next().toString());
            if(iterator.hasNext()){
                listOfFileNames.append(" | ");
            }
        }

        context.write(key, new Text(listOfFileNames.toString()));
    }
}
