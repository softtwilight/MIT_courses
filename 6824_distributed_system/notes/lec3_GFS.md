## GFS

[paper](https://pdos.csail.mit.edu/6.824/papers/gfs.pdf)

I learn a lot from this paper, you can see how may decision will make in a real world big system.  
How the assumption, based on what fact, to achieve what goals, and solution pros and cons, how to solve and avoid these cons by pratical way. So many information to achieve :   
- performance  
- scalability  
- reliability  
- availability  

Really amazing!


### Why Big Storage is hard?
performance -> sharding(many machine)  
sharding 	  -> faults  
faults  	  -> tolerance  
tolerance   -> replication  
replication -> inconsistency  
consistency -> low performance  	

### Strong consistency
behave as read and write from a single server.

### GFS goals

- big
- fast
- global
- sharding
- automatic recovery

### Master information
mapping of :  
file name   ->  array of chunk handles  (NV for not in virtual memory)
handle      ->  list of CS(chunk server, V)
					version number (NV)
					primary CS
					lease expiration

log, and checkpoint on disk.


### Read operation

1. client send (file_name, offset) to master
2. master reply H, list of chunk server, the client cache the information
3. client ask CS for data, CS return the Data

### Write operation
only record append.

if no primary on master {  
	- find up to date replicas by **version number**  
	- pick one as Primary Server  
	- increment version number, lease  
	- tells PS , #Version  
	- write to master disk  
	// 根据教授所讲， 当master 重启之后， 会发心跳到CS， and 接受它们的version number， 如果version number   
	//  低于master 保存的 version num， 就会一直等下去， 所以如果先写disk，然后master 挂了， 就可能找出所有的  
	// CS的version number 都小于master的。  
}

- Client send data to all CS including Primary and Secondary, and CS store in a buffer  
- If ok, client told Primary, all CS has data, and Primary determine the order of write to all replica.  
- If all CS write "yes", reply success. otherwise reply error, then client reissue the write.