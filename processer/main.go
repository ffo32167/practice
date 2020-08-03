package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

/* написано для практики с воркерпулами
читаем большое количество информации, затем обрабатываем в два этапа разным количеством рабочих
*/

type person struct {
	firstname string
	lastname  string
	address   string
	checkCode float32
}

const (
	filename          = "testData.csv"
	stage1Workers int = 3
	stage2Workers int = 2
)

var (
	stage1Worker = func(workerID int, jobs <-chan person, stage1Result chan<- person, wg *sync.WaitGroup) {
		defer wg.Done()
		for pers := range jobs {
			pers.checkCode = pers.checkCode / 7
			stage1Result <- pers
		}
	}
	stage2Worker = func(workerID int, stage1Result <-chan person, stage2Result chan<- person, wg *sync.WaitGroup) {
		defer wg.Done()
		for pers := range stage1Result {
			pers.checkCode = pers.checkCode / 5
			stage2Result <- pers
		}
	}
)

//  <-chan получить
//  chan<- отправить
func main() {
	jobs := make(chan person, stage1Workers)
	stage1Result := make(chan person, stage1Workers)
	stage2Result := make(chan person, stage2Workers)

	startWorkers(stage1Workers, stage1Worker, jobs, stage1Result)
	startWorkers(stage2Workers, stage2Worker, stage1Result, stage2Result)

	sendJobs(jobs, filename)

	result := readResult(stage2Result)

	fmt.Println("обработано", <-result)
}

func readResult(stageResult <-chan person) chan int {
	result := make(chan int)
	go func() {
		cnt := 0
		for p := range stageResult {
			cnt++
			if p.checkCode != 3 {
				log.Fatal("Ужас! checkCode не равен 3!", p)
			}
		}
		result <- cnt
	}()
	return result
}

func sendJobs(jobs chan person, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("can't open file", filename)
	}

	scanner := bufio.NewScanner(file)
	go func() {
		defer close(jobs)
		defer file.Close()
		for scanner.Scan() {
			columns := strings.Split(scanner.Text(), ",")
			in, _ := strconv.Atoi(columns[3])
			jobs <- person{columns[0], columns[1], columns[2], float32(in)}
		}
	}()
}

func startWorkers(workersNeeded int, worker func(int, <-chan person, chan<- person, *sync.WaitGroup), stageJobs <-chan person, stageResults chan<- person) {
	wg := new(sync.WaitGroup)
	wg.Add(workersNeeded)
	for i := 0; i < workersNeeded; i++ {
		go worker(i, stageJobs, stageResults, wg)
	}
	go func() {
		wg.Wait()
		close(stageResults)
	}()
}
