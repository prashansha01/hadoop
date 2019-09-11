import java.util.PriorityQueue;

public class PriorityQueueDemo2 {

    /*
        Program to sort students in descending order based on roll number using Priority Queue
     */
    public static void main(String[] args){

        PriorityQueue<Student> students = new PriorityQueue<Student>();
        Student s1 = new Student(5, "Rahul");
        Student s2 = new Student(2, "Priya");
        Student s3 = new Student(7, "Raj");
        Student s4 = new Student(3, "Simran");

        students.add(s1);
        System.out.println("Printing the queue: "+ students.toString());

        students.add(s2);
        System.out.println("Printing the queue: "+ students.toString());

        students.add(s3);
        System.out.println("Printing the queue: "+ students.toString());

        students.add(s4);
        System.out.println("Printing the queue: "+ students.toString());

        /*while(students.poll()!=null){
            System.out.println(students.poll());
        }*/

        for(Student s = students.poll(); s !=null; s=students.poll()){
            System.out.println(s);
        }


    }
}
