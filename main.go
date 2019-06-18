package main

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
)

func main() {
	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, getenv("BIGQUERY_PROJECT"))
	if err != nil {
		log.Fatal(err)
	}
	table := bigqueryClient.Dataset(getenv("BIGQUERY_DATASET")).Table(getenv("BIGQUERY_TABLE"))

	days := 40
	if v := os.Getenv("DAYS"); v != "" {
		days, err = strconv.Atoi(v)
		if err != nil {
			log.Fatal(err)
		}
	}
	start := time.Now().Add(-time.Hour * 24 * time.Duration(days))
	end := time.Now()

	log.Println("Deleting table...")
	table.Delete(ctx)

	log.Println("Creating table...")
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			&bigquery.FieldSchema{Name: "event_time", Type: bigquery.TimestampFieldType, Required: true},
			&bigquery.FieldSchema{Name: "event_type", Type: bigquery.StringFieldType, Required: true},
			&bigquery.FieldSchema{Name: "user_id", Type: bigquery.StringFieldType, Required: true},
			&bigquery.FieldSchema{Name: "event_properties", Type: bigquery.StringFieldType, Required: true},
			&bigquery.FieldSchema{Name: "user_properties", Type: bigquery.StringFieldType, Required: true},
		},
	}); err != nil {
		log.Fatal(err)
	}

	pr, pw := io.Pipe()
	enc := csv.NewWriter(pw)
	go func() {
		for current := start; !current.After(end); current = current.Add(24 * time.Hour) {
			log.Printf("Downloading %s...\n", current.Format("2006-01-02"))

			day := current.Format("20060102")
			url := fmt.Sprintf("https://amplitude.com/api/2/export?start=%sT00&end=%sT23", day, day)
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Fatal(err)
			}
			req.SetBasicAuth(getenv("AMPLITUDE_API_KEY"), getenv("AMPLITUDE_SECRET_KEY"))

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Fatal(resp.Status)
			}

			log.Printf("Export size: %d MiB\n", resp.ContentLength/1024/1024)
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
			}

			r, err := zip.NewReader(bytes.NewReader(data), resp.ContentLength)
			if err != nil {
				log.Fatal(err)
			}

			for _, f := range r.File {
				log.Printf("Uploading %s...\n", f.Name)

				fr, err := f.Open()
				if err != nil {
					log.Fatal(err)
				}

				gr, err := gzip.NewReader(fr)
				if err != nil {
					log.Fatal(err)
				}

				dec := json.NewDecoder(gr)
				dec.DisallowUnknownFields()

				for {
					var event RawEvent
					if err := dec.Decode(&event); err != nil {
						if err == io.EOF {
							break
						}
						log.Fatal(err)
					}

					if event.UserID == "" {
						continue
					}

					if err := enc.Write([]string{
						event.EventTime,
						event.EventType,
						event.UserID,
						string(event.EventProperties),
						string(event.UserProperties),
					}); err != nil {
						log.Fatal(err)
					}
				}

				fr.Close()
			}
		}

		enc.Flush()
		pw.Close()
	}()

	if _, err := table.LoaderFrom(bigquery.NewReaderSource(pr)).Run(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("Done.")
}

type RawEvent struct {
	ServerReceivedTime      string          `json:"server_received_time"`
	App                     int             `json:"app"`
	DeviceCarrier           string          `json:"device_carrier"`
	Schema                  int             `json:"$schema"`
	City                    string          `json:"city"`
	UserID                  string          `json:"user_id"`
	UUID                    string          `json:"uuid"`
	EventTime               string          `json:"event_time"`
	Platform                string          `json:"platform"`
	OsVersion               string          `json:"os_version"`
	AmplitudeID             int             `json:"amplitude_id"`
	ProcessedTime           string          `json:"processed_time"`
	UserCreationTime        string          `json:"user_creation_time"`
	VersionName             string          `json:"version_name"`
	IPAddress               string          `json:"ip_address"`
	Paying                  string          `json:"paying"`
	DMA                     string          `json:"dma"`
	GroupProperties         json.RawMessage `json:"group_properties"`
	UserProperties          json.RawMessage `json:"user_properties"`
	ClientUploadTime        string          `json:"client_upload_time"`
	InsertID                string          `json:"$insert_id"`
	EventType               string          `json:"event_type"`
	Library                 string          `json:"library"`
	AmplitudeAttributionIds string          `json:"amplitude_attribution_ids"`
	DeviceType              string          `json:"device_type"`
	DeviceManufacturer      string          `json:"device_manufacturer"`
	StartVersion            string          `json:"start_version"`
	LocationLng             string          `json:"location_lng"`
	ServerUploadTime        string          `json:"server_upload_time"`
	EventID                 int             `json:"event_id"`
	LocationLat             string          `json:"location_lat"`
	OSName                  string          `json:"os_name"`
	AmplitudeEventType      string          `json:"amplitude_event_type"`
	DeviceBrand             string          `json:"device_brand"`
	Groups                  json.RawMessage `json:"groups"`
	EventProperties         json.RawMessage `json:"event_properties"`
	Data                    json.RawMessage `json:"data"`
	DeviceID                string          `json:"device_id"`
	Language                string          `json:"language"`
	DeviceModel             string          `json:"device_model"`
	Country                 string          `json:"country"`
	Region                  string          `json:"region"`
	IsAttributionEvent      bool            `json:"is_attribution_event"`
	AdID                    string          `json:"adid"`
	SessionID               int             `json:"session_id"`
	DeviceFamily            string          `json:"device_family"`
	SampleRate              string          `json:"sample_rate"`
	IDFA                    string          `json:"idfa"`
	ClientEventTime         string          `json:"client_event_time"`
}

func getenv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatalf("Error: %s not set.", name)
	}
	return v
}
