package mambed

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go"
)

func TestMain(m *testing.M) {
	// create table
	initDB()

	// runing test cases
	code := m.Run()

	// cleaning
	dropTable()
	os.Exit(code)
}

func initDB() {
	conn, err := sql.Open("clickhouse", "tcp://10.4.197.101:9001?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := conn.Ping(); err != nil {
		log.Fatal(err)
	}

	if _, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS person (
			name 	   String,
			age        UInt8
		)engine=Memory
	`); err != nil {
		log.Fatal(err)
	}
}

func dropTable() {
	conn, err := sql.Open("clickhouse", "tcp://10.4.197.101:9001?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := conn.Ping(); err != nil {
		log.Fatal(err)
	}

	if _, err = conn.Exec(`DROP TABLE person`); err != nil {
		log.Fatal(err)
	}
}

type Message struct {
	Name string
	Age  int
}

// Person implememts CKJob.
type Person struct {
	Name string
	Age  uint8
}

func (p *Person) String() string {
	return p.Name
}

func (p *Person) Stmt() string {
	return personStmt
}

func (p *Person) Execute(stmt *sql.Stmt) error {
	if _, err := stmt.Exec(
		p.Name,
		p.Age,
	); err != nil {
		return err
	}
	return nil
}

func NewPerson() CKJob {
	return &Person{}
}

func validateResource(message interface{}) (*Message, error) {
	if req, ok := message.(*Message); ok {
		return req, nil
	}
	return nil, errors.New("incorrect message")
}

func (p *Person) FillData(message interface{}) error {
	req, err := validateResource(message)
	if err != nil {
		return err
	}
	p.Name = req.Name
	p.Age = uint8(req.Age)

	return nil
}

const personStmt = `INSERT INTO person (
	name, 
	age
) VALUES (?, ?)`
