Golang mfworker
----------
> Mfworker is a memory and file based task worker for Golang. 
> It's embeddable for golang project. So it's only work for single process application.

## Features
1. Custom worker numbers.
2. Custom the number of jobs stored in memory.
3. Single process.
4. Thread safe. 
5. Jobs will be store in memory first, then auto store jobs to badger db file.
6. Recoverable, when process exit, it will auto save jobs to file.

## Demo
```go
package main

import (
    "log"
    "strconv"
    "time"
    "github.com/iflamed/mfworker"
)

func main()  {
    var (
        count    uint
        maxItems uint
    )
    count = 4
    maxItems = 16
    path := "./test.db"
    queueName := "mfworkder"
    q := mfworker.NewQueue(count, maxItems, path, queueName, nil)
    q.Handler("Test", func(job *mfworker.Job) {
        time.Sleep(time.Second)
        log.Printf("the job name %s, job body %s ", job.Name, job.Payload)
    })
    q.Start()
    go func() {
        for i := 0; i < 64; i++ {
            job := &mfworker.Job{
                Name:    "Test",
                Payload: []byte("body " + strconv.Itoa(i)),
            }
            q.Dispatch(job)
        }
    }()
    <-time.After(10 * time.Second)
    q.Stop()
}
```
