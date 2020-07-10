package mambed

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestMambedProcess(t *testing.T) {
	initDB()
	must := assert.New(t)
	conf := &Config{
		MaxBatchLength:  1000,
		MaxBatchTime:    time.Second * 1,
		MaxConsumlength: 100000,
		CKAccessURI:     "tcp://10.4.197.101:9001",
	}
	worker := NewMambed(conf, NewPerson)

	go worker.Run()
	for i := 0; i < 100000; i++ {
		var req Message
		uid, _ := uuid.NewV4()
		req.Name = uid.String()
		err := worker.Consume(&req)
		must.Nil(err)
	}
	worker.Close()
	worker.Done()
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

func TestIsDataInsertSuccess(t *testing.T) {
	conn, err := sqlx.Open("clickhouse", "tcp://10.4.197.101:9001?debug=true")
	if err != nil {
		log.Fatal(err)
	}

	var p []Person
	if err := conn.Select(&p, "SELECT name, age FROM person"); err != nil {
		log.Fatal(err)
	}

	for _, row := range p {
		fmt.Println(row)
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
