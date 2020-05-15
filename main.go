package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	timeFmt   = "2006-01-02 15:04:05.000000"
	hourStart = "07:00:00.000000"
	period    = 20
	dayHour   = 24
	cnsl      = 5
)

type Candle struct {
	ticker     string
	ts         time.Time
	openPrice  float64
	maxPrice   float64
	minPrice   float64
	closePrice float64
}

func main() {
	err := pipeline()
	if err != nil {
		log.Fatalf("func pipeline crash: %s", err)
	}
}

// Функция запуска пайплайна.
func pipeline() error {
	// Парсинг входных аргументов.
	var cmd string
	flag.StringVar(&cmd, "flag", "trades.csv", "Path to the file with trades.")
	flag.Parse()

	start, err := findDate(cmd)
	if err != nil {
		return fmt.Errorf("func findDate crash: %s", err)
	}

	duration := cnsl * time.Second

	// Создание контекста с таймаутом.
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	out := make(chan struct{})

	// Запуск первой стадии и получение канала строк.
	fileChan, in, err := StageOne(ctx, cmd, start)
	if err != nil {
		return fmt.Errorf("func StageOne crash %s", err)
	}

	// Разделение одного канала на три, для каждого обработчика.
	ch5, ch30, ch240 := SeparateChan(fileChan)

	// Запуск второй стадии для свечек разного масштаба.
	cdl5 := StageTwo(ch5, 5, start)
	cdl30 := StageTwo(ch30, 30, start)
	cdl240 := StageTwo(ch240, 240, start)

	go func() {
		defer close(out)
		<-out
	}()

	in <- struct{}{}

	// Запуск третьей стадии.
	StageThree(out, cdl5, cdl30, cdl240)

	return nil
}

// Первая стадия. Принимает на вход имя файла, контекст завершения и стартовое время.
// Возвращает канал строк и возможную ошибку.
func StageOne(ctx context.Context, path string, tStart time.Time) (chan []string, chan struct{}, error) {
	out := make(chan []string)

	in := make(chan struct{})

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to open file: %s", err)
	}

	r := csv.NewReader(file)

	// Время начала торгов.
	tEnd := tStart.Add(time.Hour * time.Duration(period))

	go func(file *os.File) {
		defer file.Close()
		defer close(out)
		defer close(in)

		<-in

		for {
			record, err := r.Read()
			if err == io.EOF {
				return
			}

			if err != nil {
				fmt.Printf("unable to read line: %s\n", err)
				return
			}

			inputTime, _ := time.Parse(timeFmt, record[3])

			// Если запись после времени конца торгов,
			// то переностим стартовое время на дату следующего дня.
			if inputTime.After(tEnd) {
				tStart = tStart.Add(dayHour * time.Hour)
				tEnd = tStart.Add(time.Hour * time.Duration(period))
			}

			// Если запись попадает в промежуток, когда тогруем,
			// то отправляем валидную запись в канал,.
			if inputTime.After(tStart) && inputTime.Before(tEnd) {
				select {
				case <-ctx.Done():
					fmt.Println("ctx is done")
					return
				default:
					out <- record
				}
			}
		}
	}(file)

	return out, in, nil
}

// Запуск второй стадии. Принимаем канал для чтения и масштаб. Возвращаем выходной канал.
// Цену открытия и закрытия мы считаем как внутренний интервал.
func StageTwo(in chan []string, scale int, startTime time.Time) chan Candle {
	varTime := startTime

	// Мапа для отслеживаемых свечей.
	// Из нее будем отправлять данные после завершения периода.
	tickers := make(map[string]Candle)
	out := make(chan Candle)

	go func(in chan []string) {
		defer close(out)

		// Принимаем строки из файла через канал.
		for s := range in {
			t, err := time.Parse(timeFmt, s[3])
			if err != nil {
				fmt.Printf("unable to parse time %s\n", err)
			}

			// Если пришла запись с временем после масштаба свечки, то выгружаем а канал все хранящиеся свечки.
			if t.After(varTime.Add(time.Duration(scale) * time.Minute)) {
				varTime = varTime.Add(time.Duration(scale) * time.Minute)

				tickers = dropTickers(out, tickers)
			}

			// Если запись с временем за предеделами торгового дня, то переводим время на начало следующего.
			if t.After(startTime.Add(period * time.Hour)) {
				varTime = startTime.Add(dayHour * time.Hour)
				startTime = varTime
				tickers[s[0]] = Candle{}
			}

			// Если в мапе по тикеру храниться пустой объект, то с пришедшей записи начинаем формирование свечки.
			// Если нет, то корректируем хранящюуся свечку.
			if candle := tickers[s[0]]; (candle == Candle{}) {
				candle, err := newCandle(s, varTime)
				if err != nil {
					fmt.Printf("func newCandle crash: %s\n", err)
				}

				tickers[s[0]] = candle
			} else {
				tickers[s[0]] = changeCandle(s, tickers[s[0]])
			}
		}

		// Если канал закрыт, то выгружаем оставшиеся свечки.
		_ = dropTickers(out, tickers)
	}(in)

	return out
}

func dropTickers(out chan Candle, tickers map[string]Candle) map[string]Candle {
	for k, v := range tickers {
		if (v != Candle{}) {
			out <- v

			tickers[k] = Candle{}
		}
	}

	return tickers
}

// Третья стадия. Получаем список каналов и начинаем писать информацию из них в файлы.
func StageThree(out chan struct{}, chans ...chan Candle) {
	var wg sync.WaitGroup

	wg.Add(len(chans))

	ch5 := chans[0]
	ch30 := chans[1]
	ch240 := chans[2]

	go AppendInFile(ch5, "./candles_5m.csv", &wg)
	go AppendInFile(ch30, "./candles_30m.csv", &wg)
	go AppendInFile(ch240, "./candles_240m.csv", &wg)

	wg.Wait()
	out <- struct{}{}
}

// Функция записи данных из канала в файл.
func AppendInFile(in chan Candle, fileName string, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("unable to open file %s\n", err)
	}
	defer file.Close()

	for candle := range in {
		t := candle.ts.Format("2006-01-02T15:04:05Z")

		note := fmt.Sprintf("%s,%v,%v,%v,%v,%v\n", candle.ticker, t, candle.openPrice, candle.maxPrice, candle.minPrice, candle.closePrice)
		if _, err := file.WriteString(note); err != nil {
			fmt.Printf("unable to write in file: %s", err)
		}
	}
}

// Функция разделение одного входного канала на три выходных.
func SeparateChan(in chan []string) (chan []string, chan []string, chan []string) {
	ch5 := make(chan []string)
	ch30 := make(chan []string)
	ch240 := make(chan []string)

	go func(chan []string, chan []string, chan []string) {
		defer close(ch5)
		defer close(ch30)
		defer close(ch240)

		for s := range in {
			ch5 <- s
			ch30 <- s
			ch240 <- s
		}
	}(ch5, ch30, ch240)

	return ch5, ch30, ch240
}

// Функция создания новой свечки.
func newCandle(s []string, t time.Time) (Candle, error) {
	candle := Candle{}

	candle.ticker = s[0]
	candle.ts = t

	openP, err := strconv.ParseFloat(s[1], 64)
	if err != nil {
		return Candle{}, fmt.Errorf("unable to parse float %s", err)
	}

	candle.openPrice = openP
	candle.maxPrice = openP
	candle.minPrice = openP
	candle.closePrice = openP

	return candle, nil
}

// Изменение свечки
func changeCandle(s []string, candle Candle) Candle {
	inputPrice, err := strconv.ParseFloat(s[1], 64)
	if err != nil {
		fmt.Printf("unable to parse float %s\n", err)
	}

	if inputPrice > candle.maxPrice {
		candle.maxPrice = inputPrice
	}

	if inputPrice < candle.minPrice {
		candle.minPrice = inputPrice
	}

	candle.closePrice = inputPrice

	return candle
}

// Нахождение дня начала сбора данных.
func findDate(path string) (time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to open file: %s", err)
	}
	defer file.Close()

	r := csv.NewReader(file)

	record, err := r.Read()
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to read line: %s", err)
	}

	day := strings.Split(record[3], " ")[0]

	t, err := time.Parse(timeFmt, fmt.Sprintf("%s %s", day, hourStart))
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to parse time: %s", err)
	}

	return t, nil
}
