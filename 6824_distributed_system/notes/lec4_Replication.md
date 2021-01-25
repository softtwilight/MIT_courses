## Replication

[The Design of a Practical System for Fault-Tolerant Virtual Machines](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)

The basic idea is regard VM as a **deterministic state machine**, send log event(all input and non-determinism information) to Backup server, **deterministic replay**.  
There are some operation non-deterministic(random, or I/O interuption), extra information two keep two server sync.    
Single core, without multiple-process problem.    
Use the **output-rule** to insure consistence for client when primary shut-down.  
Use the shared disk to ensure no "split brain" problem.  
Use TCP to detect the duplicate packet when cut-over.  
To achieve robust, easy-to-use, highly automated.  


### What failure can deal with

fail-stop faults. (when sth wrong, stop execution, no error),  can't deal with soft/hard ware bugs.  

### Two way to replication
1. state transfer 
	- need two transfer may state
	- copy
	- friendly for multiple-core
2. Replicated state machine 
	- only transfer operation and input
	- reply
	- hard when VM is multiple-core


### Output rule

Whenever primary output to client, must wait unitl the backup server `ack` the last log event befor output.

If the client see the reply, the backup server must already see request.

There may be sometime Backup server `go live`, but the primary server has send the output, so may produce same packet twice. Detect by TCP protocol.

### Performace

delaying of sending packet couse performance challenge.  
- without any thread context switch when sending and receiving log entries.
- reduce the number of interruputs. 

### Primary live on

use a atomic operation `TEST-AND-SET` to update the shared disk when a server try to live as a primary.  