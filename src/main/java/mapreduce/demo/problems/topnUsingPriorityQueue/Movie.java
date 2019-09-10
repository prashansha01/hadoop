package mapreduce.demo.problems.topnUsingPriorityQueue;

import org.jetbrains.annotations.NotNull;

public class Movie implements Comparable<Movie>{

    private String data;
    private int views;

    public Movie(int views, String movieName ) {
        this.data = movieName;
        this.views = views;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public int getViews() {
        return views;
    }

    public void setViews(int views) {
        this.views = views;
    }

    @Override
    public int compareTo(@NotNull Movie movie) {
        return this.views-movie.getViews();
    }
}
