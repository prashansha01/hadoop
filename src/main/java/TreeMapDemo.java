import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;

public class TreeMapDemo {

    public static void main(String[] args){
        TreeMap<Integer, String> treemap2 = new TreeMap();
        TreeMap<Integer, String> treemap = new TreeMap<Integer, String>(new CustomComparator());

        System.out.println("Comparator for current treemap: "+ treemap.comparator());
        treemap.put(32, "ThirtyTwo");
        treemap.put(61, "SixtyOne");
        treemap.put(5, "Five");
        treemap.put(37, "ThirtySeven");
        treemap.put(24, "TwentyFour");
        treemap.put(14, "Fourteen");
        treemap.put(9, "Nine");
        treemap.put(78, "SeventyEight");
        treemap.put(41, "FortyOne");
        treemap.put(14, "Fourteen");

        System.out.println("Number Of Entries : " +treemap.size());
        System.out.println(treemap.toString());

        treemap.remove(treemap.firstKey());
        System.out.println("Number Of Entries after removing first key : " +treemap.size());
        System.out.println(treemap.toString());

  /*      TreeSet<Integer> treeset = new TreeSet<Integer>();
        treeset.add(32);
        treeset.add(51);
        treeset.add(2);
        treeset.add(2);
        treeset.add(45);
        treeset.add(76);
        treeset.add(17);
        System.out.println("Number Of Entries : " +treeset.size());
        System.out.println(treeset.toString());*/

        /*PriorityQueue<Integer> pq = new PriorityQueue<Integer>();
        pq.add(32);
        pq.add(51);
        pq.add(2);
        pq.add(2);
        pq.add(45);
        pq.add(76);
        pq.add(17);
        System.out.println("Number Of Entries : " +pq.size());
        System.out.println(pq.toString());*/

    }
}
