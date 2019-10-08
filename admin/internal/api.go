package admin

import (
	"database/sql"
	"encoding/json"
	"log"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/googlegsa/sfdb/driver/go"
)

type App struct {
        Logger  *log.Logger
        Router  *mux.Router
        DB      *sql.DB
}

func (app *App) connectDb(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	db, err := sql.Open("sfdb", string(body))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err = db.Ping(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	app.DB = db
	w.WriteHeader(http.StatusOK)
}

func (app *App) closeDb(w http.ResponseWriter, r *http.Request) {
	// TODO: check error on close action
	app.DB.Close()
	w.WriteHeader(http.StatusOK)
}

func (app *App) showTables(w http.ResponseWriter, r *http.Request) {
	// TODO: Add the check for empty or non A-Z 'db' param
	params := mux.Vars(r)
	_ = params["db"]

	rows, err := app.DB.Query("SHOW TABLES")
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	tables := []string{}

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			app.Logger.Println("WARN: could't parse table name")
			continue
		}
		tables = append(tables, name)
	}

	// TODO:
	if err = rows.Err(); err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err = json.NewEncoder(w).Encode(tables); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

//func (app *App) getTable(w http.ResponseWriter, r *http.Request) {
//	w.Header().Set("Content-Type", "application/json")
//
//	// TODO: Add the check for empty or non A-Z 'db, table' param
//	params := mux.Vars(r)
//	// TODO: Currently, we operate only with the only DB (MAIN).
//	_ = params["db"]
//	tableName := params["table"]
//
//	q := fmt.Sprintf("SELECT * FROM %s LIMIT 10", tableName)
//	result, err := app.DB.Query(q)
//	if err != nil {
//		app.Logger.Println(err.Error())
//		w.WriteHeader(http.StatusInternalServerError)
//		return
//	}
//
//	var table Table
//	for result.Next() {
//		result.Scan(&table)
//	}
//
//	json.NewEncoder(w).Encode(table)
//	w.WriteHeader(http.StatusOK)
//}

//func (app *App) query(w http.ResponseWriter, r *http.Request) {
//	w.Header().Set("Content-Type", "application/json")
//
//	params := mux.Vars(r)
//	_ = params["db"]
//
//	var query string
//	_ = json.NewDecoder(r.Body).Decode(query)
//
//	result, err := app.DB.Query(query)
//	if err != nil {
//		app.Logger.Println(err.Error())
//		w.WriteHeader(http.StatusInternalServerError)
//		return
//	}
//
//	var row driver.Rows
//	for result.Next() {
//		result.Scan(&row)
//	}
//
//	json.NewEncoder(w).Encode(row)
//	w.WriteHeader(http.StatusOK)
//}

func (app *App) exec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	_ = params["db"]

	var execStr string
	_ = json.NewDecoder(r.Body).Decode(execStr)

	_, err := app.DB.Exec(execStr)
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (app *App) createTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	_ = params["db"]
	tableName := params["table"]

	_, err := app.DB.Exec("CREATE TABLE ? IF NOT EXISTS", tableName)
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (app *App) deleteTable(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	_ = params["db"]
	tableName := params["table"]

	_, err := app.DB.Exec("DROP TABLE ? IF EXISTS", tableName)
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
