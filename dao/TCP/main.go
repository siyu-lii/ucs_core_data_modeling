package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

const ()

func main() {
	var taosDSN = "root:taosdata@tcp(62.234.16.239:6030)/" //Or 62.234.16.239:6030
	taos, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	fmt.Println("Connected to " + taosDSN + " successfully.")
	defer taos.Close()

	// create database
	res, err := taos.Exec("CREATE DATABASE IF NOT EXISTS Tehu")
	if err != nil {
		log.Fatalln("Failed to create database Tehu, ErrMessage: " + err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
	}
	fmt.Println("Create database Tehu successfully, rowsAffected: ", rowsAffected)

	// create Stable
	res, err = taos.Exec("CREATE STABLE IF NOT EXISTS Tehu.meters " +
		"(ts TIMESTAMP, temp INT, humi INT) TAGS (dnode_id INT, dp_offset INT)")
	if err != nil {
		log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create stable rowsAffected, ErrMessage: " + err.Error())
	}
	fmt.Println("Create stable Tehu.meters successfully, rowsAffected:", rowsAffected)

	// insert data, please make sure the database and table are created before
	insertQuery := "INSERT INTO " +
		"Tehu.th01 USING Tehu.meters TAGS(1,1) " +
		"VALUES " +
		"(NOW + 1a, 25, 20) " +
		"(NOW + 2a, 35, 10) " +
		"(NOW + 3a, 28, 25) " +
		"Tehu.th02 USING Tehu.meters TAGS(2, 1) " +
		"VALUES " +
		"(\"2024-9-15 14:38:05\", 38, 15) " +
		"Tehu.th03 USING Tehu.meters TAGS(3, 2) " +
		"VALUES " +
		"(\"2024-9-17 20:21:05\", 36, 10) "

	//insertQuery
	res, err = taos.Exec(insertQuery)
	if err != nil {
		log.Fatalf("Failed to insert data to Tehu.meters, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	fmt.Printf("Successfully inserted %d rows to Tehu.meters.\n", rowsAffected)

	// Query data
	//sql := "SELECT ts, temp, humi FROM Tehu.meters limit 100"
	sql := "select ts, dnode_id, temp, humi from Tehu.meters where dnode_id=1 and temp>=30 and humi<=20"
	rows, err := taos.Query(sql)
	if err != nil {
		log.Fatalf("Failed to query data from Tehu.meters, sql: %s, ErrMessage: %s\n", sql, err.Error())
	}
	for rows.Next() {
		// Add your data processing logic here
		var (
			ts       time.Time
			dnode_id int
			temp     int
			humi     int
		)
		err = rows.Scan(&ts, &dnode_id, &temp, &humi)
		if err != nil {
			log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", sql, err)
		}
		fmt.Printf("ts: %s, dnode_id: %d, temp: %d, humi : %d\n", ts, dnode_id, temp, humi)
	}
}
