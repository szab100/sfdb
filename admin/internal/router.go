package admin

import (
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var (
	cwd = ""
)

func init() {
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

	app.Router.HandleFunc("/", serveTpl)

	// rest-api
	app.Router.HandleFunc("/api/connect", app.connectDb).Methods("POST")
	app.Router.HandleFunc("/api/close", app.closeDb).Methods("POST")

	app.Router.HandleFunc("/api/{db}", app.showTables).Methods("GET")
	app.Router.HandleFunc("/api/{db}/{table}", app.getTable).Methods("GET")
	app.Router.HandleFunc("/api/{db}/{table}", app.createTable).Methods("POST")
	app.Router.HandleFunc("/api/{db}/{table}", app.deleteTable).Methods("DELETE")
	app.Router.HandleFunc("/api/{db}/{table}/query", app.query).Methods("POST")
	app.Router.HandleFunc("/api/{db}/{table}/exec", app.exec).Methods("POST")
}

// serve template files
func serveTpl(w http.ResponseWriter, r *http.Request) {
	lp := filepath.Join(cwd, "templates", "layout.html")

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

	tmpl, err := template.ParseFiles(lp, fp)
	if err != nil {
		// Log the detailed error
		log.Println(err.Error())
		// Return a generic "Internal Server Error" message
		http.Error(w, http.StatusText(500), 500)
		return
	}

	if err := tmpl.ExecuteTemplate(w, "layout", nil); err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(500), 500)
	}
}

