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
    csvDateFormat = "1/2/06"
)

func sanitizeMetricName(metric string) string {
	metric = strings.TrimSpace(metric)
	metric = strings.ReplaceAll(metric, " ", "_")
	metric = strings.ReplaceAll(metric, ".", "_")
	metric = strings.ToLower(metric)
	return metric
}

func main() {
	var dataDir, carbonAddress string
	flag.StringVar(&dataDir, "data-dir", "/home/stefan/git/COVID-19/", "Path to where the CSVs are stored.")
	flag.StringVar(&carbonAddress, "carbon", "localhost:2003", "carbon-cache address to send metrics to.")
	flag.Parse()

	conn, err := net.Dial("tcp", carbonAddress)
	if err != nil {
		//log.Fatal(err)
	}

	dataDir = path.Join(dataDir, "csse_covid_19_data", "csse_covid_19_time_series")
    cases := []string{"confirmed", "deaths"}

    for _, c := range(cases) {
        var header []string
        file := fmt.Sprintf("time_series_covid19_%s_global.csv", c)
		f, err := os.Open(path.Join(dataDir, file))
		if err != nil {
            log.Printf("Failed to open: %s. Skipping...\n", f)
			continue
		}
		r := csv.NewReader(f)
        for {
            record, err := r.Read()
            if err == io.EOF {
                break
            }
			if err != nil {
				log.Printf("Failed to parse CSV (%s)\n", err)
			}
            if header == nil {
                if !strings.HasPrefix(record[0], "Province") {
                    log.Fatalf("Invalid header in %s\n", file)
                }
                header = record
            } else {
                region := strings.ToLower(record[0])
                country := strings.ToLower(record[1])
                if len(region) == 0 {
                    region = "none"
                }
                for i := 4; i < len(record); i++ {
                    date, err := time.Parse(csvDateFormat, header[i])
                    if err != nil {
                        log.Printf("Failed to parse date: %s (%s).\n", header[i], err)
                    }
                    metric := fmt.Sprintf("covid-19.%s.%s.%s", sanitizeMetricName(country), sanitizeMetricName(region), strings.ToLower(c))
                    fmt.Fprintf(conn, "%s %s %d\n", metric, record[i], date.Unix())
                }
            }
        }
    }
}
