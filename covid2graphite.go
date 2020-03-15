package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"time"
)

const (
	fileDateLayout    = "01-02-2006"
	csvDateFormat     = "2006-01-02T15:04:05"
	csvAltDateFormat  = "1/2/2006 15:04"
	csvAltDateFormat2 = "1/2/06 15:04"
	dateLayout        = "2006-01-02"
	day               = time.Hour * 24
)

func sanitizeMetricName(metric string) string {
	metric = strings.TrimSpace(metric)
	metric = strings.ReplaceAll(metric, " ", "_")
	metric = strings.ReplaceAll(metric, ".", "_")
	metric = strings.ToLower(metric)
	return metric
}

func main() {
	var dataDir, startDate, carbonAddress string
	flag.StringVar(&dataDir, "data-dir", "/home/stefan/git/COVID-19/", "Path to where the CSVs are stored.")
	flag.StringVar(&startDate, "since", "2020-01-01", "Import metrics since that date.")
	flag.StringVar(&carbonAddress, "carbon", "localhost:2003", "carbon-cache address to send metrics to.")
	flag.Parse()
	conn, err := net.Dial("tcp", carbonAddress)
	if err != nil {
		log.Fatal(err)
	}
	dataDir = path.Join(dataDir, "csse_covid_19_data", "csse_covid_19_daily_reports")
	start, err := time.Parse(dateLayout, startDate)
	if err != nil {
		log.Fatal(err)
	}

	for ; time.Now().Sub(start) > 0; start = start.Add(day) {
		f, err := os.Open(path.Join(dataDir, start.Format(fileDateLayout)+".csv"))
		if err != nil {
			log.Println("Failed to open file for: ", start.Format(dateLayout), "skipping...")
			continue
		}
		r := csv.NewReader(f)
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Failed to parse CSV with: %s\n", err)
			}
			if len(record) < 6 {
				log.Println("Invalid row.", record)
				continue
			}
			if strings.HasPrefix(record[0], "Province") || record[2] == "Last Update" {
				continue // it's a header
			}
			var date time.Time
			date, err = time.Parse(csvDateFormat, record[2])
			if err != nil {
				date, err = time.Parse(csvAltDateFormat, record[2])
				if err != nil {
					date, err = time.Parse(csvAltDateFormat2, record[2])
					if err != nil {
						log.Printf("Failed to parse date: %s. Falling back to %s.\n", record[2], start.Format(dateLayout))
					}
				}
			}
			for i, m := range []string{"confirmed", "deaths", "recovered"} {
				if len(record[0]) == 0 {
					record[0] = "none"
				}
				metric := fmt.Sprintf("covid-19.%s.%s.%s", sanitizeMetricName(record[1]), sanitizeMetricName(record[0]), m)
				log.Printf("Adding: %s for: %s\n", metric, date.Format(dateLayout))
				fmt.Fprintf(conn, "%s %s %d\n", metric, record[i+3], date.Unix())
			}
		}
	}
}
