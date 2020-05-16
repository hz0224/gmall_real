package com.nyist.gmall.gmall_publisher.bean;


public class Option {

    String name ;

    Double value;

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    public Option() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Option{" +
                "name='" + name + '\'' +
                ", value=" + value +
                '}';
    }
}
