package mapreduce.demo.problems.youtubeAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

public class Category implements Comparable<Category> {

    private Long totalCount;
    private String category;

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

   public Category(Long totalCount, String category) {
        this.totalCount = totalCount;
        this.category = category;
    }
    public Category() {

    }

    @Override
    public int compareTo(@NotNull Category o) {
        if((o.totalCount-this.totalCount)<0) {
            return -1;
        }
        else if((o.totalCount-this.totalCount)>0){
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
