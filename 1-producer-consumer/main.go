//////////////////////////////////////////////////////////////////////
//
// Given is a producer-consumer scenario, where a producer reads in
// tweets from a mockstream and a consumer is processing the
// data. Your task is to change the code so that the producer as well
// as the consumer can run concurrently
//

package main

import (
	"fmt"
	"sync"
	"time"
)

func producer(stream Stream, job chan *Tweet) {
	for {
		tweet, err := stream.Next()
		if err == ErrEOF {
			close(job)
			return
		}

		job <- tweet
	}
}

func consumer(job chan *Tweet) {
	for t := range job {
		if t.IsTalkingAboutGo() {
			fmt.Println(t.Username, "\ttweets about golang")
		} else {
			fmt.Println(t.Username, "\tdoes not tweet about golang")
		}
	}
}

func main() {
	start := time.Now()
	stream := GetMockStream()

	job := make(chan *Tweet, 10)
	wg := sync.WaitGroup{}
	// Producer
	go producer(stream, job)

	// Consumer
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(){
			defer wg.Done()
			consumer(job)
		}()
	}

	wg.Wait()

	fmt.Printf("Process took %s\n", time.Since(start))
}
