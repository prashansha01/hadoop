import org.jetbrains.annotations.NotNull;

public class Student implements Comparable<Student> {

    private int rollNo;
    private String name;

    public Student(int rollNo, String name) {
        this.rollNo = rollNo;
        this.name = name;
    }

    public int getRollNo() {
        return rollNo;
    }

    public void setRollNo(int rollNo) {
        this.rollNo = rollNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /*@Override
    public int compareTo(@NotNull Student o) {
        return  o.getRollNo() - this.rollNo;
    }*/
    @Override
    public int compareTo(@NotNull Student o) {
        return  o.getName().compareTo(this.name);
    }

    @Override
    public String toString(){
        return this.rollNo + ", "+ this.name;
    }

}
