package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Warning: .env file not found")
	}
}

type TrafficData struct {
	ID     string  `json:"id"`
	Index  float64 `json:"index"`
	Name   string  `json:"name"`
	Number int     `json:"number"`
	Speed  float64 `json:"speed"`
}

func main() {
	// API URL and headers
	url := "https://report.amap.com/ajax/districtRank.do"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return
	}

	// Set headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")

	// Set query parameters
	q := req.URL.Query()
	q.Add("linksType", "4")
	q.Add("cityCode", "440100")
	req.URL.RawQuery = q.Encode()

	// Make HTTP request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error fetching data: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Error: received status code %d\n", resp.StatusCode)
		return
	}

	// Read and save response to file
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response body: %v\n", err)
		return
	}

	err = os.WriteFile("amapindex.json", body, 0644)
	if err != nil {
		fmt.Printf("Error writing to file: %v\n", err)
		return
	}

	// Parse JSON response
	var data []TrafficData
	err = json.Unmarshal(body, &data)
	if err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		return
	}

	// Connect to PostgreSQL database
	connStr := os.Getenv("DB_CONNECTION_STRING")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("Error starting transaction: %v\n", err)
		return
	}

	// Prepare insert statement
	stmt, err := tx.Prepare(`
		INSERT INTO gz_traffic_index 
		(zone_id, zone_name, zone_number, traffic_index, avg_speed, record_time)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		fmt.Printf("Error preparing statement: %v\n", err)
		tx.Rollback()
		return
	}
	defer stmt.Close()

	// Insert data
	for _, item := range data {
		currentTime := time.Now().Format("2006-01-02 15:04:05")
		fmt.Printf("zone: %s, index: %.2f, name: %s, number: %d, speed: %.2f, time: %s\n",
			item.ID, item.Index, item.Name, item.Number, item.Speed, currentTime)

		_, err := stmt.Exec(
			item.ID,
			item.Name,
			item.Number,
			item.Index,
			item.Speed,
			currentTime,
		)
		if err != nil {
			fmt.Printf("Error inserting data: %v\n", err)
			tx.Rollback()
			return
		}
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		fmt.Printf("Error committing transaction: %v\n", err)
		return
	}

	fmt.Println("Data inserted successfully")
}
