// Licensed to the public under one or more agreements.
// Crystal Construct Limited licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

package database

import (
	"github.com/arangodb/go-velocypack"
)

func marshal(i interface{}) ([]byte, error) {
	return velocypack.Marshal(i)
}

func unmarshal(b []byte) interface{} {
	var val interface{}
	err := velocypack.Unmarshal(b, &val)
	if err != nil {
		panic(err)
	}
	return val
}
