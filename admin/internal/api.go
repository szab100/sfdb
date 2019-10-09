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

func respondWithJSON(w http.ResponseWriter, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
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

	respondWithJSON(w, tables)
}

func (app *App) getTable(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	// TODO: Currently, we operate only with the only DB (MAIN).
	_ = params["db"]
	tableName := params["table"]

	rows, err := app.DB.Query("DESCRIBE " + tableName)
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	tableDescrs := make(map[string]string)

	for rows.Next() {
		var fieldName string
		var fieldType string
		if err = rows.Scan(&fieldName, &fieldType); err != nil {
			app.Logger.Println("could not parse table description")
			continue
		}
		tableDescrs[fieldName] = fieldType
	}

	respondWithJSON(w, tableDescrs)
}

func (app *App) query(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	_ = params["db"]

	query, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	app.Logger.Println("Query is: ", string(query))

	rows, err := app.DB.Query(string(query))
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	data := []interface{}{}

	for rows.Next() {
		row := []interface{}{}
		if err = rows.Scan(row...); err != nil {
			app.Logger.Println("could not scan row query")
			continue
		}
		app.Logger.Println("Data: ", row)
		data = append(data, row)
	}

	respondWithJSON(w, data)
}

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
