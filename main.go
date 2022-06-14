package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

func HandlePanic() { //Recovery Mechanism for the application
	a := recover()
	if a != nil {
		fmt.Println("Recovered from ", a)
	}
}

func OpenCSVFile(filePath string, ch chan interface{}, filter map[string]interface{}) bool {
	defer HandlePanic()
	result := false
	if len(filePath) <= 0 { //chek if the path has a valid length
		panic("Invalid filePath length")
	}
	file, err := os.OpenFile(os.ExpandEnv(filePath), os.O_RDONLY, 0600) //Read only file pointer, Expand environment variables within a string
	defer func() {
		err = file.Close()
		if err != nil {
			panic(err)
		}
	}()
	if err != nil { //Check if there was an error opening a file
		panic(err)
	}
	row1, err := bufio.NewReader(file).ReadSlice('"') //Read the column name
	if err != nil {
		panic(err)
	}
	_, err = file.Seek(int64(len(row1)-1), io.SeekStart)
	if err != nil {
		panic(err)
	}
	fReader := csv.NewReader(file)
	for {
		/*data, next, err := fReader.ReadLine() //read line by line since we dont know the size of the whole file.
		if err == nil && !next {              //data was read successfully
			ch <- data
			continue
		}
		if next { //If buffer for reading is too small
			panic("Buffer provided is too small")
		}
		if err == io.EOF { //End of file reached
			result = true
			break
		}
		if err != nil { //Any other error that may occur
			panic(err)
		}*/
		data, err := fReader.Read()
		if err == nil { //data was read successfully

			if len(filter) == 0 {
				ch <- data
			} else {
				unmatched := 0
				if vl, found := filter["stock_change"]; found {
					ve, _ := vl.(string)
					available, err := strconv.Atoi(data[3])
					if err == nil {
						if requested, err := strconv.Atoi(ve); err == nil {
							if available < requested {
								unmatched += 1
							}
						}
					}
				}
				if vl, found := filter["country"]; found {
					if data[0] != vl {
						unmatched += 1
					}
				}
				if vl, found := filter["sku"]; found {
					if data[1] != vl {
						unmatched += 1
					}
				}
				if vl, found := filter["name"]; found {
					if data[2] != vl {
						unmatched += 1
					}
				}
				if unmatched == 0 {
					ch <- data
				}
			}
			continue
		}
		if err == io.EOF { //End of file reached
			result = true
			break
		}
		if err != nil { //Any other error that may occur
			panic(err)
		}
	}
	return result
}

type Product struct {
	Country string `json:country`
	Sku     string `json:sku`
	Name    string `json:name`
	Stock   string `json:stock_change`
}

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", os.Getenv("IP")+":"+os.Getenv("PORT"))
			defer HandlePanic()
			content := r.Header.Get("Content-Type")
			contentType, _, err := mime.ParseMediaType(content)
			if err != nil {
				http.Error(w, "Invalid Header", http.StatusInternalServerError)
				return
			}
			if len(contentType) <= 0 || contentType != "application/json" {
				http.Error(w, "Expected json data", http.StatusUnsupportedMediaType)
				return
			}
			next.ServeHTTP(w, r)
		})
}

func QueryProductsFunc(w http.ResponseWriter, r *http.Request) {
	defer HandlePanic()
	defer r.Body.Close() //Prevent memory leak

	data, err := io.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	filter := map[string]interface{}{}
	err = json.Unmarshal(data, &filter)
	if err != nil {
		http.Error(w, "Error Parsing json data", http.StatusInternalServerError)
		return
	}
	ch := make(chan interface{})
	ch1 := make(chan interface{})
	products := []Product{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		OpenCSVFile("$HOME/Desktop/Jumia/challenge_files/file_1.csv", ch, filter)

	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		OpenCSVFile("$HOME/Desktop/Jumia/challenge_files/file_2.csv", ch1, filter)

	}()
	go func() {
		defer HandlePanic()
		for {
			select {
			case dataChannel0 := <-ch:
				{
					val, isString := dataChannel0.([]string)
					if isString {
						var product = Product{val[0], val[1], val[2], val[3]}
						products = append(products, product)
					}
				}
			case dataChannel1 := <-ch1:
				{
					val, isString := dataChannel1.([]string)
					if isString {
						var product = Product{val[0], val[1], val[2], val[3]}
						products = append(products, product)
					}
				}
			default:
				{

				}
			}
		}
	}()
	wg.Wait()
	json.NewEncoder(w).Encode(products)
}

func MakeOrderFunc(w http.ResponseWriter, r *http.Request) {
	defer HandlePanic()
	defer r.Body.Close() //Prevent memory leak

	data, err := io.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	filter := map[string]interface{}{}
	err = json.Unmarshal(data, &filter)

	if err != nil {
		http.Error(w, "Error Parsing json data", http.StatusInternalServerError)
		return
	}
	if _, found := filter["stock_change"]; !found {
		http.Error(w, "Stock quantity field required", http.StatusForbidden)
		return
	}
	d, err := json.Marshal(filter)
	if err != nil {
		http.Error(w, "Error Parsing json data", http.StatusInternalServerError)
		return
	}
	resp, err := http.Post("http://"+os.Getenv("IP")+":"+os.Getenv("PORT")+"/products", "application/json", bytes.NewBuffer(d))
	if err != nil {
		http.Error(w, "Error Making requests", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()
	var existProd = []Product{}
	json.NewDecoder(resp.Body).Decode(&existProd)
	if len(existProd) == 0 {
		http.Error(w, "Quantity requested not available", http.StatusForbidden)
		return
	}
	json.NewEncoder(w).Encode(existProd)

}

func main() {
	err := godotenv.Load() //Load the env file
	if err != nil {
		log.Fatal("Error loading  .env file")
	}

	r := mux.NewRouter()                                                                                   //Defining the routes
	r.Handle("/products", Middleware(http.HandlerFunc(QueryProductsFunc))).Methods("POST").Schemes("http") //search using sku
	r.Handle("/makeorder", Middleware(http.HandlerFunc(MakeOrderFunc))).Methods("POST").Schemes("http")

	s := &http.Server{
		Addr:           os.Getenv("IP") + ":" + os.Getenv("PORT"),
		Handler:        r,
		ReadTimeout:    15 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go func() { //Run server in a go routine to prevent blocking
		if err := s.ListenAndServe(); err != nil {
			log.Fatal(err)
		}

	}()
	ch := make(chan os.Signal, 1) //CTRL+C
	signal.Notify(ch, os.Interrupt)
	<-ch
	log.Printf("Server is down")
}
