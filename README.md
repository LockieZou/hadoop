## Hadoop
### 基于springboot环境搭建的hadoop项目  
环境版本：  
1、Springboot 2.1.0  
2、Hadoop3.1.1  
3、jdk1.8  
4、虚拟机CentOS7  
  
### 操作步骤  
首先开启虚拟机CentOS，我的虚拟机IP地址是 192.168.187.128 所以访问 http://192.168.2.2:50070/ 就是hdfs的文件管理系统  
访问hdfs的文件管理系统：  
![hdfs文件系统](https://github.com/LockieZou/hadoop/blob/master/20181129150935.png)  
使用Postman进行RPC调用：  
![hdfs文件系统](https://github.com/LockieZou/hadoop/blob/master/20181129151544.png)  
  
    
博客地址： [java代码操作hdfs](https://blog.csdn.net/zxl646801924/article/details/84615604)  
[java代码操作mapReduce](https://blog.csdn.net/zxl646801924/article/details/85005506)  
  
    
  
统计明星微博粉丝数、关注数参考博客 ![统计明星微博](https://www.cnblogs.com/zlslch/p/6164435.html)  
  
  
  
为了方便测试采用rpc方式调用mapreduce：  
统计微博明星粉丝数，关注数，微博数，分文件输出。访问 localhost:9000/hadoop/reduce/weibo  
未完待续.... 



