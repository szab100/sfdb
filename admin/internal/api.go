package admin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	_ "github.com/googlegsa/sfdb/driver/go"
	"github.com/gorilla/mux"
)

type App struct {
	Logger *log.Logger
	Router *mux.Router
	DB     *sql.DB
}

type TableField struct {
	Name      string `json:"name"`
	FieldType string `json:"field_type"`
}

type CreateTableArg struct {
	TableName string       `json:"table_name"`
	Fields    []TableField `json:"fields"`
}

type Table struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

func respondJson(app *App, w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		app.Logger.Println("Failed to encode result: ", err.Error())
	}
}

func processRows(app *App, rows *sql.Rows, w http.ResponseWriter) (table Table) {
	var err error
	table.Columns, err = rows.Columns()
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	for rows.Next() {
		row_contents := make([]interface{}, len(table.Columns))
		row_contents_ptr := make([]interface{}, len(table.Columns))
		for i, _ := range row_contents_ptr {
			row_contents_ptr[i] = &row_contents[i]
		}
		if err := rows.Scan(row_contents_ptr...); err != nil {
			app.Logger.Println("WARN: could't parse rows: ", err.Error())
			continue
		}
		table.Rows = append(table.Rows, row_contents)
	}

	if err = rows.Err(); err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	return table
}

func (app *App) connectDb(w http.ResponseWriter, r *http.Request) {
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
	if err := app.DB.Close(); err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
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

func respondError(app *App, w http.ResponseWriter, err error) {
	app.Logger.Println(err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	return
}

func (app *App) showTables(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	db := params["db"]
	if strings.TrimSpace(db) == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	rows, err := app.DB.Query("SHOW TABLES")
	if err != nil {
		respondError(app, w, err)
	}

	tables := []string{}
	if cols, _ := rows.Columns(); len(cols) == 0 {
		respondJson(app, w, tables)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			app.Logger.Println("WARN: could't parse table name")
			continue
		}
		tables = append(tables, name)
	}

	respondJson(app, w, tables)
}

func (app *App) getTable(w http.ResponseWriter, r *http.Request) {
	// TODO: Add the check for empty or non A-Z 'db, table' param
	params := mux.Vars(r)
	// TODO: Currently, we operate only with the only DB (MAIN).
	_ = params["db"]
	tableName := params["table"]

	q := fmt.Sprintf("SELECT * FROM %s;", tableName)
	app.Logger.Println(q)

	rows, err := app.DB.Query(q)
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if cols, _ := rows.Columns(); len(cols) == 0 {
		respondJson(app, w, Table{})
		return
	}

	defer rows.Close()

	table := processRows(app, rows, w)
	respondJson(app, w, table)
}

func (app *App) query(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	_ = params["db"]

	query, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	rows, err := app.DB.Query(string(query))
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if cols, _ := rows.Columns(); len(cols) == 0 {
		respondJson(app, w, Table{})
		return
	}
	defer rows.Close()

	table := processRows(app, rows, w)
	respondJson(app, w, table)
}

func (app *App) exec(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	_ = params["db"]

	var execStr string
	_ = json.NewDecoder(r.Body).Decode(execStr)

	_, err := app.DB.Exec(execStr)
	if err != nil {
		respondError(app, w, err)
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) createTable(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	_ = params["db"]

	var arg CreateTableArg
	err := json.NewDecoder(r.Body).Decode(&arg)
	if err != nil {

		return
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var field_str = ""

	// TODO: Add checks?
	for _, f := range arg.Fields {
		if field_str != "" {
			field_str += ", "
		}
		field_str += f.Name + " " + f.FieldType
	}

	_, err = app.DB.Exec("CREATE TABLE ?(?) IF NOT EXISTS", arg.TableName, field_str)
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) deleteTable(w http.ResponseWriter, r *http.Request) {
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

func (app *App) describeTable(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	_ = params["db"]
	tableName := params["table"]

	rows, err := app.DB.Query("DESCRIBE " + tableName)
	if err != nil {
		app.Logger.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	tableDescrs := make(map[string]string)

	if cols, _ := rows.Columns(); len(cols) == 0 {
		respondJson(app, w, tableDescrs)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var fieldName string
		var fieldType string
		if err = rows.Scan(&fieldName, &fieldType); err != nil {
			app.Logger.Println("could not parse table description")
			continue
		}
		tableDescrs[fieldName] = fieldType
	}

	respondJson(app, w, tableDescrs)
}
