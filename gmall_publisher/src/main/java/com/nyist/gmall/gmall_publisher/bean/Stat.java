package com.nyist.gmall.gmall_publisher.bean;


import java.util.List;

//饼图
public class Stat {
    String title ;
    List<Option> options;

    public Stat() {
    }

    public Stat(String title, List<Option> options) {
        this.title = title;
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    @Override
    public String toString() {
        return "Stat{" +
                "title='" + title + '\'' +
                ", options=" + options +
                '}';
    }
}
