package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/googlegsa/sfdb/driver/go/admin"
	_ "github.com/googlegsa/sfdb/driver/go"
)

var (
	port = 3000
	logger *log.Logger
)

func init() {
	logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	router := mux.NewRouter()

	app := &admin.App{
		Logger: logger,
		Router: router,
	}

	app.RegisterHandlers()

	log.Println("Listening on", port, "port..")
	http.ListenAndServe(fmt.Sprintf(":%d", port), router)
}
