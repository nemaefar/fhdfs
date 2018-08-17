# FHDFS
**FHDFS** - is an utility for manipulating HDFS as file storage in a parallel way.
Instead of uploading multiple files in one thread, as native client does,
this utility does it in multiple threads.
Main drawback is that because HDFS is not supporting uploading
one file as number of blocks in its default implementation.
So we have to upload single file as number of separated blocks.

#### Usage
```bash
fhdfs [cluster] [cmd] [from] [to]
```

ex. `fhdfs prd put ./file /tmp/`

#### Possible commands
put - put file in HDFS
get - get file from HDFS
putV - put file to HDFS with version suffix (by default 3 versions is kept)
getV - get file from HDFS with the latest version, which is fully written

#### Version suffix
When uploading a file to HDFS - there is a suffix added to it:

```bash
$ fhdfs prd putV file /tmp/test_distributed/
$ hdfs-prd dfs -ls /tmp/test_distributed/
       0 2018-03-19 17:53 /tmp/test_distributed/_SUCCESS_file
-rw-------   3 43221160 2018-03-16 13:49 /tmp/test_distributed/file.ver.1521470999929
-rw-------   3 58028800 2018-03-17 14:33 /tmp/test_distributed/file.ver.1521471050161
-rw-------   3 67108032 2018-03-18 16:08 /tmp/test_distributed/file.ver.1521471176054
```

Also there is a new file created, which is pointing at the last one.
When uploading the new version, the oldest one is erased.
When loading file from HDFS - only the last version is used. So it is totally
clear for the client.

```bash
$ fhdfs prd getV /tmp/test_distributed/file ./
$ ls -lah
-rw------- 1 67108032 мар 19 19:41 file
``` 

#### How do the upload of huge files work?
We were not able to hack into native HDFS client deep enough, so an alternative
solution was selected. The main pattern for those people, who suffers from slow HDFS speed - 
uses it as file storage, so they are trying to put into HDFS a file,
which consists of more than one block. So we are breaking it into separate smaller files,
and uploading them independently.

So we could now get a good speed, but we lose an backward compatibility with the native client

Ex. When uploading file with 2GB size:

```bash
$ ls -lah
-rw-------   1 2,0G мар 19 18:08 wavs.tgz
$ fhdfs prd put wavs.tgz /tmp/test_distributed/
 
 
$ hdfs-prd dfs -ls /tmp/test_distributed/
drwxr-xr-x   0 2018-03-19 18:02 /tmp/test_distributed/wavs.tgz.distributed
 
 
$ hdfs-prd dfs -ls /tmp/test_distributed/wavs.tgz.distributed
-rw-------   3          0 2018-03-02 19:23 /tmp/test_distributed/wavs.tgz.distributed/_ATTRIBUTES
-rw-r--r--   3  268435456 2018-03-19 18:01 /tmp/test_distributed/wavs.tgz.distributed/part-00000
-rw-r--r--   3  268435456 2018-03-19 18:01 /tmp/test_distributed/wavs.tgz.distributed/part-00001
-rw-r--r--   3  268435456 2018-03-19 18:02 /tmp/test_distributed/wavs.tgz.distributed/part-00002
-rw-r--r--   3  268435456 2018-03-19 18:01 /tmp/test_distributed/wavs.tgz.distributed/part-00003
-rw-r--r--   3  268435456 2018-03-19 18:02 /tmp/test_distributed/wavs.tgz.distributed/part-00004
-rw-r--r--   3  268435456 2018-03-19 18:01 /tmp/test_distributed/wavs.tgz.distributed/part-00005
-rw-r--r--   3  268435456 2018-03-19 18:02 /tmp/test_distributed/wavs.tgz.distributed/part-00006
-rw-r--r--   3  194340092 2018-03-19 18:01 /tmp/test_distributed/wavs.tgz.distributed/part-00007
``` 

Such file probably won't be processable by standart Hadoop tools,
so for it's extraction from HDFS you should also use this utility,
so it would convert this back to a file

```bash
$ fhdfs prd get /tmp/test_distributed/wavs.tgz ./
$ ls -lah
-rw-------   1 2,0G мар 19 18:08 wavs.tgz
```