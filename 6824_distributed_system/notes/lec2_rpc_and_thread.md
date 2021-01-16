## Lecture2 : RPC and Threads 

### Threads (go rountine)
- Each thread has own stack

#### Why use thread ?
- IO concurrency
- Parallelism
- Convenience (Backgroud, check sth for a period time ...)

Or event-driven style.

#### Thread challenges 
- share data (RACE condition, use lock)
```go
 mutex.Lock
 n = n + 1
 mutex.Unlock
```
- coordination (For example: channels, sync.Cond, waitGroup)
- Deadlock (T1 -> T2 && T2 -> T1) 
 
Use mutex to concurrent
```go
type fetchState struct {
	mu sync.Mutex
	fetched map[string]bool
}

func ConcurrentMutex(url string, fetcher Fetcher, f * fetchState) { 
	 f.mu.Lock()
	 already := f.fetched[url]
	 f.fetched[url] = true
	 f.mu.Unlock()
	 
	 if already {
	 	return
	 }
	 
	 urls, err := fetcher.Fetch(url)
	 if err != nil {
* 	 	return
	 }
	 var done sync.WaitGroup
	 for _, u := range urls {
	 	done.Add(1)
	 	go func(u string) {
	 		// insure the Done is always called 
	 		defer done.Done()
	 		ConcurrentMutex(u, fetcher, f)
	 	}(u)
	 }
	 done.Wait()
	 return
}

```

`go run -race xxx.go` detect the race condition. 


Use channel to concurrent
```
func worker(url string, ch chan []string, fetch Fetcher) {
	urls, err := fetcher.Fetch(url)
	if err != nil {
		ch <- []string{}
	} else {
		ch <- urls
	}
}


func master(ch chan []string, fetcher Fetcher) {
	n := 1
	fetched := make(map[string]bool)
	for urls := range ch {
		for _, u := range urls {
			if fetched[u] == false {
				fetched[u] = true
				n += 1
				go worker(u, ch, fetcher)
			}
			
		}
		n--
		if n == 0 {
			break
		}
	}
}

func ConcurrentChannel(url string, fetcher Fetcher) {
	ch := make(chan []string)
	go func() {
		ch <- []string{url}
	}()
	master(ch, fetcher)
}

```