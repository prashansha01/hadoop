package mapreduce.demo.problems.topNRecords;

import java.util.Comparator;

public class LongComparator implements Comparator<Long> {




    @Override
    public int compare(Long o1, Long o2) {
        if(o1>o2){
            return 1;
        } /*else if(o1==o2){
            return 0;
        } */else{
            return -1;

        }
    }
}
