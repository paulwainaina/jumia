package main

import (
	"testing"
)
type TestPath struct{
	Path string
	Expected bool
}
var paths=[]TestPath{
	{"$HOME/Desktop/Jumia/challenge_files/file_1.csv",true},
	{"",false},
	{"/home/john/Desktop/Jumia/challenge_files/file_2.csv",true},
	{"/home/john/Desktop1/Jumia/challenge_files/file_2.csv",false},
}
func TestOpenFile(t *testing.T){
	for index, path:=range paths{
		ch :=make(chan interface{})
		t.Logf("%v = %v",index,path.Path)
		if OpenCSVFile(path.Path,ch)!=path.Expected{
			t.Fatal("OpenCSVFile() function failed")
		}
	}
}