package main

import (
	"testing"
)
type TestPath struct{
	Path string
	Expected bool
}

func TestDeleteCSVRecord(t *testing.T){
	path:="$HOME/Desktop/Jumia/sample.csv"
	record:=[]string{"eg","cbf87a9be799","Foster-Harrell Table","35"}
	if !DeleteCSVRecord(path,record){
		t.Fatalf("Somewthing went wrong")
	}
}
