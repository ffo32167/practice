package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
)

// errorgroup
// ctx

/* написано для практики с генераторами и пайпланами
создаем большое количество информации, затем пишем в интерфейс
*/

type person struct {
	firstname string
	lastname  string
	address   string
	checkCode int
}

const (
	filename  = "testData.csv"
	stringLen = 10
	count     = 20000000 // 1 Gb = 20000000
)

func main() {
	var (
		chooseDestination bool
		destination       io.ReadWriteCloser
	)
	// аварийная остановка "горшочек не вари"
	stopper := newStopper()
	// генерируем некоторое количество персон
	data := newPersonsList(stringLen, count, stopper)
	// записываем только в файл, TCP для красоты
	switch chooseDestination {
	case true:
		destination = createTCPConn()
	default:
		destination = createFile(filename)
	}
	// записываем в формат csv
	cnt := encodeCsv(data, destination)

	fmt.Println("обработано", <-cnt)
}

// newStopper возвращает канал, который останавливает генератор
func newStopper() (stopper chan struct{}) {
	stopper = make(chan struct{})
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("нажмите Enter для остановки")
	go func() {
		for scanner.Scan() {
			stopper <- struct{}{}
		}
	}()
	return stopper
}

// создаёт случайную строку
func makeRandString(stringLen int) string {
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZабвгдеёжзиклмнопрстуфхцчшщъыьэюя")
	var b strings.Builder
	for i := 0; i < stringLen; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

// создаем новую персону
func newPerson(stringLen int) person {
	return person{
		makeRandString(stringLen),
		makeRandString(stringLen),
		makeRandString(stringLen),
		105,
	}
}

// записать туда, не знаю куда, для примера интерфейса
func createTCPConn() io.ReadWriteCloser {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		log.Fatal("can't create connection: ", err)
	}
	return conn
}

// вывод в файл
func createFile(filename string) io.ReadWriteCloser {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("can't reach destination: ", err)
	}
	return file
}

// создаем список персон
func newPersonsList(stringLen, count int, stopper <-chan struct{}) (pers chan person) {
	pers = make(chan person)
	go func() {
		pers <- person{"Иван", "Иванов", "Москва", 105}
		for i := 1; i < count; i++ {
			select {
			case <-stopper:
				fmt.Println("остановлено пользователем")
				close(pers)
				return
			default:
				pers <- newPerson(stringLen)
			}
		}
		close(pers)
	}()
	return pers
}

// сохраняет данные в csv формате
func encodeCsv(personsList chan person, destination io.ReadWriteCloser) (count chan int) {
	count = make(chan int)
	w := csv.NewWriter(destination)
	var record person
	go func() {
		cnt := 0
		for record = range personsList {
			if err := w.Write([]string{record.firstname, record.lastname, record.address, strconv.Itoa(record.checkCode)}); err != nil {
				destination.Close()
				log.Fatalln("error writing record to csv: ", err)
			}
			cnt++
		}
		count <- cnt
		w.Flush()
		if err := w.Error(); err != nil {
			log.Println("problems with flush: ", err)
		}
		destination.Close()
	}()
	return count
}
