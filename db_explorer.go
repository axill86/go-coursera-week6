package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"net/url"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type TablesContext struct {
	Tables map[string]TableInfo
	TableNames []string
}
type TableInfo struct {
	Name string
	Id string
	Fields []FieldInfo
}

type FieldInfo struct {
	Name string
	Type string
}

func (field *FieldInfo) getValueFromString( value string) (interface{}, error) {
	var result interface{}
	var err error
	switch field.Type {
	case "varchar":
		err = nil
		result = value
	case "text":
		err = nil
		result = value
	case "int":
		result, err = strconv.Atoi(value)
	}
	return result, err
}

func (tablesCtxt *TablesContext) containsTable(table string) bool{
	_, ok := tablesCtxt.Tables[table]
	return ok
}

func (table *TableInfo) prepareRow() []interface{} {
	row := make([]interface{}, len(table.Fields))
	for i, field := range table.Fields {

		switch field.Type {
		case "varchar":
			row[i] = new(sql.NullString)
		case "text":
			row[i] = new(sql.NullString)
		case "int":
			row[i] = new(sql.NullInt64)

		}

	}
	return row
}

func (table *TableInfo) prepareInsertQuery() string {
	values := make([]string, len(table.Fields))
	placeholders := make([]string, len(table.Fields))
	for i, field := range table.Fields {
		values[i] = field.Name
		placeholders[i] = "?"
	}
	return fmt.Sprintf("insert into %s (%s) values (%s)", table.Name, strings.Join(values, ", "), strings.Join(placeholders, ", "))
}

func (table *TableInfo) prepareUpdateQuery(params map[string]interface{}) string {
	values := make([]string, 0)
	for k := range params {
		values = append(values, fmt.Sprintf("%s = ?", k) )
	}
	return fmt.Sprintf("update %s set %s where %s = ?", table.Name, strings.Join(values, ","),  table.Id)
}

func (table *TableInfo) prepareInsertParameters(params map[string]interface{}, skipId bool) []interface{} {
	fmt.Printf("preparing parameters %v\n", params)
	result := make([]interface{}, len(table.Fields))
	for i, field := range table.Fields {
		if table.Id == field.Name && skipId {
			continue
		}
		if params[field.Name] == nil {
			result[i] = nil
		} else {
			result[i] = params[field.Name]
		}
	}
	return result
}

func (table *TableInfo) prepareUpdateParameters(params map[string]interface{})  []interface{} {
	result := make([]interface{}, 0)
	for _, v := range params {
		result = append(result, v)
	}
	return result
}


func (table *TableInfo) transformRow(row []interface{}) map[string]interface{} {
	item := make(map[string]interface{}, len(row))
	for i, v := range row {
		switch v.(type) {
		case *sql.NullString:
			if value, ok := v.(*sql.NullString); ok {
				if value.Valid {
					item[table.Fields[i].Name] = value.String
				} else {
					item[table.Fields[i].Name] = nil
				}

			}
		case *sql.NullInt64:
			if value, ok := v.(*sql.NullInt64); ok {
				if value.Valid {
					item[table.Fields[i].Name] = value.Int64
				} else {
					item[table.Fields[i].Name] = nil
				}

			}
		}
	}
	return item
}
func NewDbExplorer(db *sql.DB) (http.Handler, error) {

	tablesContext, err := initContext(db)
	serverMux := http.NewServeMux()
	if err != nil {
		panic(err)
	}
	serverMux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {

		switch request.Method {
		case http.MethodGet:
			path := request.URL.Path
			if path == "/" {
				result, err := json.Marshal(map[string]interface{}{"response": map[string]interface{}{"tables": tablesContext.TableNames}})
				if err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					return
				}
				writer.Write(result)
				return
			}
			fragments := strings.Split(path, "/")

			switch len(fragments) {
			case 2:
				//return table info
				// /$table
				tableName := fragments[1]
				if !tablesContext.containsTable(tableName) {
					result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
					writer.WriteHeader(http.StatusNotFound)
					writer.Write(result)
					return
				}

				limit := 5
				offset := 0

				if request.URL.Query().Get("limit") != "" {
					limit, err = strconv.Atoi(request.URL.Query().Get("limit"))
					if err != nil {
						writer.WriteHeader(http.StatusBadRequest)
						return
					}
				}
				if request.URL.Query().Get("offset") != "" {
					offset, err = strconv.Atoi(request.URL.Query().Get("offset"))
					if err != nil {
						writer.WriteHeader(http.StatusBadRequest)
						return
					}
				}

				rows, err := getRows(db, tablesContext.Tables[tableName], limit, offset)
				if err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					println(err.Error())
					return
				}
				result, err := json.Marshal(
					map[string]interface{}{"response": map[string]interface{}{"records": rows}})
				if err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					println(err.Error())
					return
				}
				writer.Write(result)
			case 3:
				table := fragments[1]
				id := fragments[2]
				if !tablesContext.containsTable(table) {
					writer.WriteHeader(http.StatusNotFound)
					println(err.Error())
					return
				}
				rows, err := getRow(db, tablesContext.Tables[table], id)
				if err != nil {
					writer.WriteHeader(http.StatusNotFound)
					result, _ := json.Marshal(map[string]string {"error": "record not found"})
					writer.Write(result)
					return
				}
				result, err := json.Marshal(
					map[string]interface{}{"response": map[string]interface{}{"record": rows}})
				writer.Write(result)

			}
		case http.MethodDelete:
		case http.MethodPost:
			path := request.URL.Path
			fragments := strings.Split(path, "/")
			tableName := fragments[1]
			id := fragments[2]
			if !tablesContext.containsTable(tableName) {
				result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
				writer.WriteHeader(http.StatusNotFound)
				writer.Write(result)
				return
			}
			table := tablesContext.Tables[tableName]

			decoder := json.NewDecoder(request.Body)
			requestParams := make(map[string]interface{}, len(table.Fields))
			decoder.Decode(&requestParams)
			fmt.Printf("Got parameters %#v\n", requestParams)
			table = tablesContext.Tables[tableName]
			result, err := updateRow(db, table, id, requestParams)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				println (err.Error())
				return
			}
			resultBytes, _ := json.Marshal(map[string]interface{}{"response": map[string]interface{}{"updated": result}})
			writer.Write(resultBytes)
		case http.MethodPut:
			path := request.URL.Path
			fragments := strings.Split(path, "/")
			tableName := fragments[1]
			if !tablesContext.containsTable(tableName) {
				result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
				writer.WriteHeader(http.StatusNotFound)
				writer.Write(result)
				return
			}
			table := tablesContext.Tables[tableName]

			decoder := json.NewDecoder(request.Body)
			requestParams := make(map[string]interface{}, len(table.Fields))
			decoder.Decode(&requestParams)
			fmt.Printf("Got parameters %#v\n", requestParams)
			result, err := insertRow(db, tablesContext.Tables[tableName], requestParams)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				println (err.Error())
				return
			}
			resultBytes, _ := json.Marshal(map[string]interface{}{"response": map[string]interface{}{"id": result}})
			writer.Write(resultBytes)
		}
	})
	return serverMux, nil
}

func (table *TableInfo) extractParams(values url.Values) map[string]interface{} {
	result := make(map[string]interface{})
	for _, field := range table.Fields {
		fmt.Printf("checking field %s\n with value %s\n ", field.Name, values[field.Name])
		if len(values[field.Name])==0 {

			result[field.Name] = nil
		} else {
			v, _ := field.getValueFromString(values[field.Name][0])
			result[field.Name] = v
		}
	}
	return result
}

func getRow(db *sql.DB, table TableInfo, id interface{}) (map[string]interface{}, error) {
	query := fmt.Sprintf("select * from %s where %s = ?", table.Name, table.Id)
	data:=table.prepareRow()
	row:=db.QueryRow(query, id)
	err := row.Scan(data...)
	if err != nil {
		return nil, err
	}
	return table.transformRow(data), nil
}
func getTables(db *sql.DB) ([]string, error) {

	rows, err := db.Query("SHOW TABLES")

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		result = append(result, tableName)

	}
	return result, nil
}

func insertRow(db *sql.DB, table TableInfo, params map[string]interface{}) (int64, error) {
	query := table.prepareInsertQuery()
	println(query)
	queryParams := table.prepareInsertParameters(params, true)
	fmt.Printf("parameters %v\n", queryParams)
	res, err := db.Exec(query, queryParams...)
	if err!=nil {
		return 0, err
	} else {
		result, _ := res.LastInsertId()
		return result, nil
	}
}

func updateRow(db *sql.DB, table TableInfo, id interface{}, params map[string]interface{}) (int64, error) {
	query := table.prepareUpdateQuery(params)
	println(query)
	queryParams := table.prepareUpdateParameters(params)
	queryParams = append(queryParams, id)
	fmt.Printf("parameters %v\n", queryParams)
	res, err := db.Exec(query, queryParams...)
	if err!=nil {
		return 0, err
	} else {
		result, _ := res.RowsAffected()
		return result, nil
	}
}

func initContext(db *sql.DB) (*TablesContext, error) {
	tables, err := getTables(db)
	if err != nil {
		return nil, err
	}
	result := new(TablesContext)
	result.TableNames = tables
	result.Tables = make(map[string]TableInfo, len(tables))
	for _, table := range tables {
		rows, err :=db.Query("SELECT column_name, if (column_key='PRI', true, false) as 'key', DATA_TYPE from information_schema.columns where  table_name = ? and table_schema=database()", table)
		if err != nil {
			return nil, err
		}
		var keyName string
		fields := make([]FieldInfo, 0)
		for rows.Next() {
			isKey := new(bool)
			f := new(FieldInfo)
			rows.Scan(&f.Name, isKey, &f.Type)
			if *isKey {
				keyName = f.Name
			}
			fields = append(fields, *f)
		}
		fmt.Printf("%#v", fields)
		result.Tables[table] = TableInfo{
			Name:table,
			Id:keyName,
			Fields:fields,
		}
		rows.Close()
	}
	return result, nil
}



func getRows(db *sql.DB, table TableInfo, limit int, offset int) ([]interface{}, error) {
	rows, err := db.Query(fmt.Sprintf("select * from %s limit %d offset %d", table.Name, limit, offset))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []interface{}{}
	for rows.Next() {

		row := table.prepareRow()
		rows.Scan(row...)
		result = append(result, table.transformRow(row))
	}

	return result, nil
}

