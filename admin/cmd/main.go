package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	_ "github.com/googlegsa/sfdb/driver/go"
	"github.com/googlegsa/sfdb/driver/go/admin"
	"github.com/gorilla/mux"
)

var (
	port        = 3000
	logger      *log.Logger
	uiDirectory = "/admin/ui/dist"
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
	http.ListenAndServe(fmt.Sprintf(":%d", port), handlerWrapper(router))
}

func handlerWrapper(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api") {
			setupResponse(&w, r)
			if (*r).Method == "OPTIONS" {
				return
			}
			next.ServeHTTP(w, r)
		} else {
			// Serve static content
			cwd_, _ := os.Getwd()
			filesDir := cwd_ + uiDirectory
			if strings.HasPrefix(r.URL.Path, "/app") {
				// Client is trying to refresh the page or open a deep link
				r.URL.Path = "/"
			}
			http.FileServer(http.Dir(filesDir)).ServeHTTP(w, r)
		}
	})
}

func setupResponse(w *http.ResponseWriter, req *http.Request) {
	(*w).Header().Set("Accept", "application/json")
	(*w).Header().Set("Content-Type", "application/json")
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	(*w).Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
}
