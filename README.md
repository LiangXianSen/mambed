# Mambed

Mambed is a clickhouse Injector which implements `Worker`. On other hands Mambed consumes message from up-stream cyclic batch-insert to clickhouse. There is a important thing before Mambed works, you have to implements `CKJob`.

```go 
// Job is a CKjob Factory type which is NewMambed() requires.
type Job func() CKJob

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
```



Firstly, implememts `CKJob`, then new a Mambed instance. Binds CKJob on Mambed, runs Mambed worker.

Declare a struct then implememts CKJob:

```go
// Declare a Job type
func NewPerson() CKJob {
	return &Person{}
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
```



New Mambed instance requires, `Config` & `Job` type; 

```go
conf := &Config{
  MaxBatchLength: 1000,
  MaxBatchTime:   time.Second * 1,
  CKAccessURI:    "tcp://10.4.197.101:9001",
}

worker := NewMambed(conf, NewPerson)
go worker.Run()
worker.Close()
worker.Done()
```



You can define any `CKJob` , underlie on Mambed. Binds Mambed runs.  The best choice is register in [worker](https://github.com/LiangXianSen/worker-manager) package.

 Examples:

- same up-stream distribute to multi-Jobs.

```go
manager := worker.NewWorkerManager()
visitor := NewMambed(conf, NewPerson)
tracker := NewMambed(conf, NewTrace)

manager.Register(
  visitor,
  tracker,
)

go manager.RunOnDistribute()
for i := 0; i < 100; i++ {
  manager.Consume(i)
}
manager.Exit()
```

- For efficiency, same `CKJob` consume up-stream together.

```go
manager := worker.NewWorkerManager()
v1 := NewMambed(conf, NewPerson)
v2 := NewMambed(conf, NewPerson)

manager.Register(
  v1,
  v2,
)

go manager.RunOnCoWork()
for i := 0; i < 100; i++ {
  manager.Consume(i)
}
manager.Exit()
```

