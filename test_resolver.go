package main

import (
	"fmt"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func main() {
	_, err := protoregistry.GlobalFiles.FindFileByPath("google/type/datetime.proto")
	fmt.Println("Error:", err)
}
