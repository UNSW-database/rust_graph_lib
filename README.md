# rust\_graph\_lib

A graph libary written in Rust. 

##Setup for hdfs reading support

###0. Explanations for `build` and `running` stage in `hdfs_lib`
The function for reading files from `hdfs` is based on a library [`hdfs-rs`](https://github.com/hyunsik/hdfs-rs). Because the library is not update for a few years, so I fixed some bugs in the source code and stored in `src/io/hdfs//hdfs_lib`. The project is regard it as a local crate. (Just as `Cago.toml` shows: `hdfs={path="src//io//hdfs//hdfs_lib"}`).  
* In the library, we were calling`libhdfs C APIs`[(docs here)](http://hadoop.apache.org/docs/r3.0.0/hadoop-project-dist/hadoop-hdfs/LibHdfs.html) (supported by hadoop) to implement functions. And encapsulate the `libhdfs C APIs` in the library. 
* In the path `hdfs_lib/src/native`, there are static library(`libhdfs.a`) and shared object(`libhdfs.so`) for calling `C APIs` in rust. It helps to guarantee that the project will compile successfully even without the hadoop environment.  
* In the file `hdfs_lib/build.rs`, we use `build.rs`[(docs here)](https://doc.rust-lang.org/cargo/reference/build-scripts.html#outputs-of-the-build-script) to pass environment variable `rustc-link-search` to prompt compiler to find the static and shared object.  
* In the running time for calling `libhdfs C APIs`, it will using `libhdfs.so`,`libjvm.so`,`Java Environment` and `Hadoop jars`.So,
please ensure that you have finished the following steps, if you want to use the functions for `hdfs`.

###1. Download hadoop and environment variables
1.1 Requirement:
* Hadoop version >= [2.6.5](http://mirror.bit.edu.cn/apache/hadoop/common/hadoop-2.6.5/)
* Java >=1.8
* Linux Environment
* In the running time of `libhdfs C APIs`
* Checking in the hadoop you have installed contains `libhdfs.so` in path `$HADOOP_HOME/lib/native/`.(In the pre-build version, hadoop contains it by default)
* Checking in the java you have installed contains `libjvm.so` in path `$JAVA_HOME/jre/lib/amd64/server/`.  

1.2 Environment variables:  
Edit shell environment as following command:
```
vim ~/.bashrc

# Change the path to your own and appending to the file
export JAVA_HOME=/{YOUR_JAVA_INSTALLED_PATH}
export JRE_HOME=${JAVA_HOME}/jre 
export HADOOP_HOME=/{YOUR_HADOOP_INSTALLED_PATH} 
export LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native:${JAVA_HOME}/jre/lib/amd64/server #for libhdfs.o linking
CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib 
CLASSPATH=${CLASSPATH}":"`find ${HADOOP_HOME}/share/hadoop | awk '{path=path":"$0}END{print path}'` # hadoop's jars 
export CLASSPATH 
export PATH=${JAVA_HOME}/bin:$HOME/.cargo/bin:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH

#flush the environment variable to all shell session
source ~/.bashrc
```

###2. Configuring a pseudo hadoop and hdfs environment
Of course, you can build a real cluster by yourself. What we need in the code is the `hdfs path` and `port`.  
  
First of all, entering the configure files directory:`cd $HADOOP_HOME/etc/hadoop`  
  
2.1 Configure `core-site.xml`  
```xml
<configuration>
<property>
    <name>hadoop.tmp.dir</name>
    <value>file:/usr/local/hadoop/tmp</value>
    <description>Abase for other temporary directories.</description>
</property>
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
</property>
<property>
    <name>hadoop.http.staticuser.user</name>
    <value>cy</value>
</property>
</configuration>
```  

2.2 Configure `hdfs-site.xml`   
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/usr/local/hadoop/tmp/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/usr/local/hadoop/tmp/dfs/data</value>
  </property>
  <property>
     <name>dfs.permissions.enabled</name>
     <value>false</value>
  </property>
</configuration>
```  

2.3 Configure `hadoop-env.sh` (Non-essential. Only for JAVA_HOME can't find during starting hdfs)
```
export JAVA_HOME=/{YOUR_JAVA_INSTALLED_PATH}
```  
###3. Starting hdfs and checking hdfs status  
* Starting hdfs: `./$HADOOP_HOME/sbin/start-dfs.sh`  
You'll get a output as following if you are successful:  
```
Starting namenodes on [localhost]
Starting datanodes
Starting secondary namenodes [{MACHINE-NAME}]
``` 
* And you can use `jps` command to verify hdfs status.
```
jps
17248 Jps
16609 DataNode
16482 NameNode
4198 Main
16847 SecondaryNameNode
```
* Now you can open a explorer to visit hdfs page  
`http://localhost:9870/dfshealth.html#tab-overview`  
The port maybe different in different version hadoop. Please check on hadoop website. 

###4. Testing and using hdfs support
For now, you can using `hdfs support` in this library to read from local pseudo hdfs cluster(or real hdfs cluster).
* In order to avoid tests failure in this library. We mark the tests for `hdfs support` as `ignore`.  
So, if you want to test them, please using the following command to test the `hdfs support` independently:  
`cargo test -- --ignored`  
