import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class IteratorDemo {
    public static void main(String[] args){
        String[] arr = {"One", "Two", "Three", "Four"};
        List<String> input = new ArrayList<String>(Arrays.asList(arr));
        System.out.println("Input: "+ input);
        StringBuilder stringBuilder = new StringBuilder();
        Iterator itr = input.iterator();
        while(itr.hasNext()){
            stringBuilder.append(itr.next());

            if(itr.hasNext()){
                stringBuilder.append(" | ");
            }
        }
        System.out.println("Output: "+ stringBuilder);
        /*ArrayList<String> list = new ArrayList<String>();
            list.add("JavaFx");
            list.add("Java");
            list.add("WebGL");
            list.add("OpenCV");
            Iterator iterator = list.iterator();
            StringBuilder sb = new StringBuilder();
            while(iterator.hasNext()) {
                sb.append(iterator.next());
            }
            System.out.println(sb);*/

    }
}
