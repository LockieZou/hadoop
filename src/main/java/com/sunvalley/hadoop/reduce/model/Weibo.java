package com.sunvalley.hadoop.reduce.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 类或方法的功能描述 : 统计明星微博
 *
 * @author: logan.zou
 * @date: 2019-01-04 15:28
 */
public class Weibo implements WritableComparable<Weibo> {
    // 微博粉丝数
    private int friends;
    // 微博关注
    private int followers;
    // 发微博数目
    private int num;

    public Weibo() {
    }

    public Weibo(int friends, int followers, int num) {
        this.friends = friends;
        this.followers = followers;
        this.num = num;
    }

    public void set(int friends, int followers, int num) {
        this.friends = friends;
        this.followers = followers;
        this.num = num;
    }


    @Override
    public int compareTo(Weibo weibo) {
        return weibo.getFriends() - this.friends;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(friends);
        output.writeInt(followers);
        output.writeInt(num);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        friends = input.readInt();
        followers = input.readInt();
        num = input.readInt();
    }

    public int getFriends() {
        return friends;
    }

    public void setFriends(int friends) {
        this.friends = friends;
    }

    public int getFollowers() {
        return followers;
    }

    public void setFollowers(int followers) {
        this.followers = followers;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}

