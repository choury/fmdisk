fmdisk
=========

A fuse disk framework for linux

**自己从其他地方上传的文件可以正常读取,但是显示为只读文件，本程序上传的文件只能由本程序读写，并不能由其他程序读写**

原理：将文件切分成小块文件，修改的时候只需要替换其中某个数据块来达到修改文件的目的。每个文件生成一个[[base64]].def 结尾的目录，目录下面meta.json文件保存了文件的元信息，其中包括所有数据块文件的列表

* 你的linux内核需要装载fuse模块(lsmod | grep fuse)
* 目前最多上传20G文件！！！！！

usage
--------------
实现stub_api.h里面的一系列函数


build
--------------
要想编译这个项目，你需要：

以下工具

* cmake
* make
* gcc, g++(需支持c++11)
* 并且安装以下软件包:
libcurl4 libcurl4-openssl json-c libfuse2 libssl 和相应的dev包（如果有）

编译方式：
```
mkdir build && cd build
cmake ..
make
```
