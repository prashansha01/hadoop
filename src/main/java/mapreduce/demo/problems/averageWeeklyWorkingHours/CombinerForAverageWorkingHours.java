package mapreduce.demo.problems.averageWeeklyWorkingHours;

import mapreduce.demo.common.NumPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerForAverageWorkingHours extends Reducer<Text, NumPair, Text, NumPair> {

    @Override
    public void reduce(Text maritalStatus, Iterable<NumPair> numpairs, Context context) throws IOException, InterruptedException {
        System.out.println("=Inside Combiner ======================================");
        long count = 0;
        Double totalWorkingHour = 0.0d;

        for(NumPair numpair : numpairs){
            System.out.println("Inside Combiner for loop: Numpair "+ numpair.toString());
            totalWorkingHour += numpair.getSum().get();
            count += numpair.getCount().get();
        }
        System.out.println("Marital Status: " + maritalStatus);
        System.out.println("Total working hours "+totalWorkingHour +" and count : "+count);
        Double value = totalWorkingHour/count;
        context.write(maritalStatus, new NumPair(totalWorkingHour, count));
    }

}
