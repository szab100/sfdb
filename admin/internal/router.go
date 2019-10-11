package admin

import (
	//"flag"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var (
	cwd = ""
)

func init() {
	// Uncomment to enable verbose
	// flag.Set("logtostderr", "true")
	// flag.Set("stderrthreshold", "WARNING")
	// flag.Set("v", "5")
	// flag.Parse()

	cwd_, err := os.Getwd()
	cwd = cwd_ + "/admin/"
	if err != nil {
		log.Fatal(err)
	}
}

func (app *App) RegisterHandlers() {
	// static

	log.Println("Registering handlers..")

	fs := http.FileServer(http.Dir(cwd + "static"))

	app.Router.PathPrefix("/static/").Handler(
		http.StripPrefix("/static/", fs))

	app.Router.HandleFunc("/", serveTpl).Methods("GET")
	app.Router.HandleFunc("/{page}.html", serveTpl).Methods("GET")

	// rest-api
	app.Router.HandleFunc("/api/connect", app.connectDb).Methods("POST")
	app.Router.HandleFunc("/api/close", app.closeDb).Methods("POST")

	app.Router.HandleFunc("/api/{db}", app.showTables).Methods("GET")
	app.Router.HandleFunc("/api/{db}/{table}", app.getTable).Methods("GET")
	app.Router.HandleFunc("/api/{db}/create", app.createTable).Methods("POST")
	app.Router.HandleFunc("/api/{db}/{table}", app.deleteTable).Methods("DELETE")
	app.Router.HandleFunc("/api/{db}/{table}/query", app.query).Methods("POST")
	app.Router.HandleFunc("/api/{db}/exec", app.exec).Methods("POST")
	app.Router.HandleFunc("/api/{db}/{table}/describe", app.describeTable).Methods("POST")
}

// serve template files
func serveTpl(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		r.URL.Path = "index.html"
	}

	fp := filepath.Join(cwd, "templates", filepath.Clean(r.URL.Path))

	// Return a 404 if the template doesn't exist
	info, err := os.Stat(fp)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
	}

	// Return a 404 if the request is for a directory
	if info.IsDir() {
		http.NotFound(w, r)
		return
	}

	// Send headers
	w.Header().Set("Content-Type", "text/html")

	streamContent, err := ioutil.ReadFile(fp)
	b := bytes.NewBuffer(streamContent)
	if _, err := b.WriteTo(w); err != nil {
		fmt.Fprintf(w, "%s\n", err)
	}
}
