import java.util.Arrays;
import java.util.List;

public class Test {

    public static void main(String[]args){
        String input = "1;Anshu;Ranu";
        String[] array = input.split(";");
        System.out.println("no of items:"+array.length);
        List<String> list = Arrays.asList(array);
        System.out.println("Printing...");
        for(String inp:list){
            System.out.println(inp);
        }

        String homeDirProp = System.getProperty("hadoop.home.dir");
        String homeDirEnv = System.getenv("HADOOP_HOME");
        System.out.println("hadoop.home.dir="+homeDirProp+" HADOOP_HOME="+homeDirEnv);
    }
}
