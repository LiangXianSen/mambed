package mambed

import (
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMambedProcess(t *testing.T) {
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
