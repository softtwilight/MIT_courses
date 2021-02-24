## The Go Memory Model
[mem](https://golang.org/ref/mem)

`sync` and `sync/atomic` package


### Happens Before
reorder does not change behavior.

With a single goroutine, the happens-before order is expressed by program.

Reads and writes larger than a single machine word behave as multiple machine-sized operation in an unspecified order.

### Synchronization

#### Initialization

> If a package p imports package q, the completion of q's init functions happens before the start of any of p's.

> The start of the function main.main happens after all init functions have finished.


#### Goroutine creation
go statement happends before goroutine's execution begins.

the exit of goroutine has no guarantee.


#### Channel communication
- send before receive.  
- close happens before receive zero.  

- A receive from an unbuffered channel happens before the send on that channel completes.  
- The kth receive on a channel with capacity C happens before the k+Cth send from that channel completes.  

#### Locks
`sync.Mutex` and `sync.RWMutex`

For any sync.Mutex or sync.RWMutex variable l and n < m, call n of l.Unlock() happens before call m of l.Lock() returns.

#### Once

he sync package provides a safe mechanism for **initialization** in the presence of multiple goroutines through the use of the Once type. Multiple threads can execute once.Do(f) for a particular f, but only one will run f(), and the other calls block until f() has returned.

A single call of f() from once.Do(f) happens (returns) before any call of once.Do(f) returns.