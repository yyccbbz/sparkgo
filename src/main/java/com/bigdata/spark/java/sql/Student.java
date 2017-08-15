package com.bigdata.spark.java.sql;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-15
 * Time: 18:01
 * Description: RDD使用的JavaBean
 */
public class Student implements Serializable {

    private static final long serialVersionUID = 1332794873562868157L;

    private int id;
    private String name;
    private int age;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
