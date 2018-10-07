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
	"time"

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

	var Subject = "John"

	fmt.Printf("Q: Who does %+v know?\n", Subject)
	JohnFriends, _ := db.Get(Subject, "Knows", nil)
	fmt.Println("A:", db.Materialize(JohnFriends))

	fmt.Printf("Q: Who does %+v know?\n", db.Materialize(JohnFriends))
	foaf, _ := db.Get(JohnFriends, "Knows", nil)
	fmt.Println("A:", db.Materialize(foaf))

	fmt.Printf("Q: How old is %+v?\n", db.Materialize(foaf))
	foafAge, _ := db.Get(foaf, "Age", nil)
	fmt.Println("A:", db.Materialize(foafAge))

	// Delete triple
	db.Delete("Jane", "Knows", "Emily")

	traversalOptions := database.TraversalOptions{}

	t := time.Now()
	// A traversal starting with people John knows
	err:= db.Traverse("John", "Knows", nil, traversalOptions, func(subjectId []byte, predicateId []byte, objectId []byte, state database.State) (database.State, [][]byte, error) {
		// Print out the path for each traversal
		fmt.Println(db.Materialize(state.Path))
		// Get the next triples to traverse
		next, _ := db.Get(objectId, "Knows", nil)
		// pass the state to the next traversal
		return state, next, nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("Traversl took: ", time.Since(t))

	//Delete entity
	db.DeleteEntity("Emily")
}
