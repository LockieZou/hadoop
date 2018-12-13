package com.sunvalley.hadoop.hdfs.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * 类或方法的功能描述 : 用户实体类
 *
 * @author: logan.zou
 * @date: 2018-12-05 15:11
 */
public class User implements Writable {
    private String username;
    private Integer age;
    private String address;

    public User() {
    }
    public User(String username, Integer age, String address) {
        this.username = username;
        this.age = age;
        this.address = address;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 把对象序列化
        output.writeChars(username);
        output.writeInt(age);
        output.writeChars(address);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // 把序列化的对象读取到内存中
        username = input.readUTF();
        age = input.readInt();
        address = input.readUTF();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "User{" +
                "username='" + username + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                '}';
    }
}

