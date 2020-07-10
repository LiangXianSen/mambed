package mambed

import (
	"database/sql"
	"errors"
	"log"
	"time"
)

// CKJob is a clickhouse job to tell Mambed how it works
// requires you to define a clickhouse stmt (clickhouse SQL)
// declares a struct represents the item you will insert
// fills fields waiting Mambed Commit().
type CKJob interface {
	// Stmt returns clickhouse SQL stmt.
	Stmt() string
	// FillData receive message, declares your struct then fill them.
	FillData(data interface{}) error
	// Execute fills fields the stmt needs.
	Execute(*sql.Stmt) error
	// String for output of the item.
	String() string
}

// Job is a CKjob Factory type which is NewMambed() requires.
type Job func() CKJob

// Mambed is a clickhouse Injector which implements Worker.
type Mambed struct {
	MaxBatchLength int
	DB             *sql.DB
	Job            Job
	JobCh          chan CKJob
	ch             chan interface{}
	done           chan struct{}
	commit         chan struct{}
	Stmt           string
	running        bool
	*time.Ticker
}

// Config contains all arguments Mambed needs.
type Config struct {
	MaxBatchLength  int
	MaxConsumlength int
	MaxBatchTime    time.Duration
	CKAccessURI     string
}

const defaultMaxConsumingLength = 10000

// Run reads message from channel doing job on background.
func (m *Mambed) Run() {
	m.running = true
	go m.batchInsert()

	for req := range m.ch {
		job := m.Job()
		err := job.FillData(req)
		if err != nil {
			log.Printf("mambed consuming: %s", err)
			continue
		}
		m.Insert(job)
	}

	m.done <- struct{}{}
}

// Insert previously inserts data into channel waiting for batch insert.
func (m *Mambed) Insert(job CKJob) {
	select {
	case m.JobCh <- job:
	default:
		m.commit <- struct{}{}
		defer func() {
			m.JobCh <- job
		}()
	}
}

// batchInsert cyclic execute Commit()
// based on MaxBatchLength & MaxBatchTime one of them achieved
func (m *Mambed) batchInsert() {
	for {
		select {
		case <-m.Ticker.C:
			if m.JobChLength() <= 0 {
				continue
			}
			if err := m.Commit(); err != nil {
				log.Printf("batching commit err: %s", err)
			}
			log.Printf("time hit, committed.")
		case <-m.commit:
			if err := m.Commit(); err != nil {
				log.Printf("batching commit err: %s", err)
			}
			log.Printf("full channel, committed.")
		}
	}
}

// Commit batch insert items into clickhouse.
func (m *Mambed) Commit() error {
	tx, err := m.DB.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(m.Stmt)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < m.MaxBatchLength; i++ {
		job, ok := <-m.JobCh
		if !ok {
			break
		}
		if err := job.Execute(stmt); err != nil {
			log.Printf("%s insert into clickhouse failed: %s", job, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// JobChLength returns how much CKJob waiting.
func (m *Mambed) JobChLength() int {
	return len(m.JobCh)
}

// ChannelLength returns the whole Mambed Consuming channel length.
func (m *Mambed) ChannelLength() int {
	return len(m.ch)
}

// SetConsumingLength sets max consuming channel length.
func (m *Mambed) SetConsumingLength() error {
	if m.running {
		return errors.New("cannot change during Mambed runing")
	}
	m.ch = make(chan interface{}, defaultMaxConsumingLength)
	return nil
}

// waitJobProcess finishs rest of CKJob before exit.
func (m *Mambed) waitJobProcess() {
	if m.JobChLength() == 0 {
		return
	}

	log.Printf("worker exit waiting for all job done.")

	tx, err := m.DB.Begin()
	if err != nil {
		log.Printf("last commit err: %s", err)
	}

	stmt, err := tx.Prepare(m.Stmt)
	if err != nil {
		log.Printf("last commit err: %s", err)
	}
	defer stmt.Close()

	for job := range m.JobCh {
		if err := job.Execute(stmt); err != nil {
			log.Printf("%s insert into clickhouse failed: %s", job, err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("last commit err: %s", err)
	}
}

// cleanCKJob sends quit signal to CKJob processes.
func (m *Mambed) cleanCKJob() {
	m.Ticker.Stop()
	close(m.commit)
	close(m.JobCh)
}

// Done waits all processes done.
func (m *Mambed) Done() {
	<-m.done
	m.cleanCKJob()
	m.waitJobProcess()
}

// Close sends quit signal.
func (m *Mambed) Close() {
	close(m.ch)
}

// Consume receive message send to channel.
func (m *Mambed) Consume(message interface{}) error {
	select {
	case m.ch <- message:
		return nil
	case <-time.After(time.Second * 1):
		return errors.New("mambed queue is overloaded")
	}
}

// NewMambed retruns Mambed instance which implememts Worker.
func NewMambed(conf *Config, job Job) *Mambed {
	ckConn, err := sql.Open("clickhouse", conf.CKAccessURI)
	if err != nil {
		panic(err)
	}

	if err := ckConn.Ping(); err != nil {
		panic(err)
	}

	if conf.MaxConsumlength <= 0 {
		conf.MaxConsumlength = defaultMaxConsumingLength
	}

	return &Mambed{
		DB:             ckConn,
		MaxBatchLength: conf.MaxBatchLength,
		Job:            job,
		Stmt:           job().Stmt(),
		JobCh:          make(chan CKJob, conf.MaxBatchLength),
		done:           make(chan struct{}),
		commit:         make(chan struct{}),
		ch:             make(chan interface{}, conf.MaxConsumlength),
		Ticker:         time.NewTicker(conf.MaxBatchTime),
	}
}
