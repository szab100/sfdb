package admin

import (
	//"flag"

	"log"
	"os"
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
	log.Println("Registering api handlers..")
	app.Router.HandleFunc("/api/connect", app.connectDb).Methods("POST")
	app.Router.HandleFunc("/api/ping", app.ping).Methods("GET")
	app.Router.HandleFunc("/api/close", app.closeDb).Methods("POST")

	app.Router.HandleFunc("/api/{db}", app.showTables).Methods("GET")
	app.Router.HandleFunc("/api/{db}", app.query).Methods("POST")
	app.Router.HandleFunc("/api/{db}/exec", app.exec).Methods("POST")
	app.Router.HandleFunc("/api/{db}/{table}", app.getTable).Methods("GET")
	app.Router.HandleFunc("/api/{db}/{table}", app.createTable).Methods("POST")
	app.Router.HandleFunc("/api/{db}/{table}", app.deleteTable).Methods("DELETE")
	app.Router.HandleFunc("/api/{db}/{table}/describe", app.describeTable).Methods("GET")
}
