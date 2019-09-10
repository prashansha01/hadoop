package mapreduce.demo.problems.youtubeAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

public class Category implements Comparable<Category> {

    private LongWritable totalCount;
    private Text category;

    public LongWritable getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(LongWritable totalCount) {
        this.totalCount = totalCount;
    }

    public Text getCategory() {
        return category;
    }

    public void setCategory(Text category) {
        this.category = category;
    }

    public Category(LongWritable totalCount, Text category) {
        this.totalCount = totalCount;
        this.category = category;
    }
    public Category() {

    }

    @Override
    public int compareTo(@NotNull Category o) {
        if((int)(this.totalCount.get()-o.totalCount.get())<0) {
            return -1;
        }
        else if((int)(this.totalCount.get()-o.totalCount.get())>0){
            return 1;
        }else {
            return 0;
        }
    }

    @Override
    public String toString(){
        return "Category: "+this.category+" Count: "+this.totalCount;
    }
}
