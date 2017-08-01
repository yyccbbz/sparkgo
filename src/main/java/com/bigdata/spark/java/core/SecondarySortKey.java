package com.bigdata.spark.java.core;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-24
 * @Time: 15:28
 * @Description: 自定义二次排序的key
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    private static final long serialVersionUID = -2910506859907074935L;

    //首先在自定义的key里面，定义需要进行排序的列
    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    /**
     * 自定义比较方法
     */
    @Override
    public boolean $less(SecondarySortKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() &&
                this.second < that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() &&
                this.second > that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst() &&
                this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() &&
                this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

}
