# Distributed File System (Simplified HDFS)
A simplified HDFS implementation using Golang without MapReduce.

Complete description: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html

## Key features of the project:

1. Parallel retrievals: large files will be split into multiple chunks. Client applications retrieve these chunks in parallel using goroutines.
2. Replication: each file will be replicated to provide a level of fault tolerance.
3. Interoperability: the DFS will use Google Protocol Buffers to serialize messages. This allows other applications to easily implement your wire format.
4. Persistence: once a file has been acknowledged as stored, you should be able to terminate the entire DFS, restart it, and still have access to the stored files.

## Three main components in this project:
1. Controller
   &nbsp;&nbsp;&nbsp;&nbsp; - Responsible to manage resources just like NameNode in HDFS.
   &nbsp;&nbsp;&nbsp;&nbsp; - Contains information about files, directories and where they are stored
   &nbsp;&nbsp;&nbsp;&nbsp; - List of active storage node
   &nbsp;&nbsp;&nbsp;&nbsp; - Store file index to the disk
2. Storage Node
   &nbsp;&nbsp;&nbsp;&nbsp; - Send heartbeat to the controller every 5 seconds
   &nbsp;&nbsp;&nbsp;&nbsp; - Store chunks in the disk (size of each chunk is 128 mb)
   &nbsp;&nbsp;&nbsp;&nbsp; - Send replication to other storage node
   &nbsp;&nbsp;&nbsp;&nbsp; - Keep track on how many spaces available, retrievals and storages
3. Client
   &nbsp;&nbsp;&nbsp;&nbsp; - Divide files into chunks
   &nbsp;&nbsp;&nbsp;&nbsp; - Retrieving files in parallel
   &nbsp;&nbsp;&nbsp;&nbsp; - Able to handle these following operations: ls, put, get, delete and usage

## How to run this program:
### Run manually
Controller:
&nbsp;&nbsp;&nbsp;&nbsp; - `go run server/controller.go -port <this_port>`<br>
Storage Node:
&nbsp;&nbsp;&nbsp;&nbsp; - `go run storage/storage_node.go <storage_dir> -port <this_port> <cont_host:port>`<br>
Client:
&nbsp;&nbsp;&nbsp;&nbsp; - Get: `go run client/client.go -get <hdfs_filepath> <local_filepath> <cont_host:port>`<br>
&nbsp;&nbsp;&nbsp;&nbsp; - Put: `go run client/client.go -put <local_filepath> <hdfs_filepath> <cont_host:port>`<br>
&nbsp;&nbsp;&nbsp;&nbsp; - Delete: `go run client/client.go -delete <hdfs_filepath> <cont_host:port>`<br>
&nbsp;&nbsp;&nbsp;&nbsp; - LS: `go run client/client.go -ls <hdfs_filepath> <cont_host:port>`<br>
&nbsp;&nbsp;&nbsp;&nbsp; - USAGE: `go run client/client.go -usage <cont_host:port>`<br>

### Run with shell script:
To run controller and storage nodes:
`$ ./start-cluster.sh`
To kill controller and storage nodes:
`$./stop-cluster.sh`

## Process Diagram:
### LS
![](https://i.imgur.com/47CR7Jd.png)



### Delete
![](https://i.imgur.com/IWeCF4z.png)


### Usage
![](https://i.imgur.com/pnMnq1p.png)


### Get
![](https://i.imgur.com/ovBSotX.png)


### Put
![](https://i.imgur.com/Cy8m7Jn.png)

<br></br>
## Project Retrospective

1. How many extra days did you use for the project?
    - I use two extra days for the project

2. Given the same goals, how would you complete the project differently if you didn’t have any restrictions imposed by the instructor? This could involve using a particular library, programming language, etc. Be sure to provide sufficient detail to justify your response.
    - In terms of programming language, I think I would do it with Golang again. The goroutines make multi-threading much simpler compared to other languages. One thing that I might change is that instead of creating my own file tree data structure, I will instead using available library provided online. One other thing that I would change is when I am implementing persistence file index on the controller, I would serialize it using protobuf instead of gob because gob is go specific.

3. Let’s imagine that your next project was to improve and extend P1. What are the features/functionality you would add, use cases you would support, etc? Are there any weaknesses in your current implementation that you would like to improve upon? This should include at least three areas you would improve/extend.
    - First, I would improve the replication. In the current implementation, if a storage node died, all the replication files in that node is gone and won't be replicated to other storage nodes that are alive. Second, In the current implementation, the chunk size is constant which is 128 mb. With this implementation, if the file is too big it will split into too many chunks and it will takes a while to store them to the storage nodes. Lastly, I would implement a better algorithm to assign chunks to the storage nodes instead of using math.Random() function.

4. Give a rough estimate of how long you spent completing this assignment. Additionally, what part of the assignment took the most time?
    - Approximately, it takes at least 40 hours forme to complete this assignment. Part that takes me the longest is when I am implementing file system tree data structure. The second longest is when I am implementing PUT and GET operation.

5. What did you learn from completing this project? Is there anything you would change about the project?
    - The main thing I learned on this project is how to use channel and tick in golang.

6. If you worked as a group, how did you divide the workload? What went well with your team, and what did not go well?
    - N/A (Working on my own)


