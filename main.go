// Licensed to the public under one or more agreements.
// Crystal Construct Limited licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

package main

import (
	"fmt"

	"./database"
)

func main() {
	db := database.New()
	defer db.Close()

	err := db.Put("Andrei", "Knows", "Claire")
	if err != nil {
		panic(err)
	}
	err = db.Put("Claire", "Pet", "Goldfish")
	if err != nil {
		panic(err)
	}
	err = db.Put("Andrei", "Knows", "Jan")
	if err != nil {
		panic(err)
	}
	err = db.Put("Claire", "Child", "Dylan")
	if err != nil {
		panic(err)
	}
	err = db.Put("Jan", "Pet", "Cat")
	if err != nil {
		panic(err)
	}
	people, err := db.Get("Andrei", "Knows", nil)
	if err != nil {
		panic(err)
	}
	children, err := db.Get(people, "Child", nil)
	if err != nil {
		panic(err)
	}
	parent, err := db.Get(nil, "Child", children)
	if err != nil {
		panic(err)
	}
	pets, err := db.Get(parent, "Pet", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(db.Materialize(children))
	fmt.Println(db.Materialize(parent))
	fmt.Println(db.Materialize(people))
	fmt.Println(db.Materialize(pets))

}
