// Licensed to the public under one or more agreements.
// Crystal Construct Limited licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	"./database"
)

func main() {
	db := database.New("data")
	defer db.Close()
	file, _ := os.Open("testdata/testdata.csv")
	defer file.Close()
	rdr := csv.NewReader(file)
	for {
		line, err := rdr.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		db.Put(line[0], line[1], line[2])
	}

	fmt.Println("Who does John know?")
	JohnFriends, _ := db.Get("John", "Knows", nil)
	fmt.Println(db.Materialize(JohnFriends))

	fmt.Println("Who do they know?")
	foaf, _ := db.Get(JohnFriends, "Knows", nil)
	fmt.Println(db.Materialize(foaf))

	fmt.Println("How old are they?")
	foafAge, _ := db.Get(foaf, "Age", nil)
	fmt.Println(db.Materialize(foafAge))

}
