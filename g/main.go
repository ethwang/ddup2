package main

import (
	"fmt"
	"sync"
)

func main() {
	fmt.Println("1")

	c1 := make(chan int, 1)
	c2 := make(chan int, 1)
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}

	fmt.Println(-1)
	c2 <- 0
	for i := 0; i < 5; i++ {

		go func() {
			wg1.Add(1)
			defer wg1.Done()

			v := <-c2
			nv := v + 1
			fmt.Println("g1: ", nv)
			c1 <- nv
		}()
		go func() {
			wg2.Add(1)
			defer wg2.Done()

			v := <-c1
			nv := v + 1
			fmt.Println("g2: ", nv)
			c2 <- nv
		}()
	}
	wg1.Wait()
	wg2.Wait()
}
