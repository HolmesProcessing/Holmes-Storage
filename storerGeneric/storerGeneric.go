package storerGeneric

import (
	"time"
)

/*
This file contains structs to represent all default
collections and interfaces.
If you need to extend these for your database specific
implementation add a wrapper to your specific storer.../*.go file,
don't change these structs here!
*/

type DBConnector struct {
	IP             string
	Port           int
	User           string
	Password       string
	Database       string
	StatusDatabase string
}

type Storer interface {
	// Cretes the database. this is separate from Initialize()
	// because Initialize() requires the database to exist
	CreateDB(c []*DBConnector) error
	CreateDBStatus(c []*DBConnector) error

	// Initializes the connection and the Storer object
	// ip, port, user, passwort, db name
	Initialize([]*DBConnector) (Storer, error)
	InitializeStatus([]*DBConnector) (Storer, error)

	// Is called to setup the db on the very first run
	// to create initial collections (if necessary)
	Setup() error
	SetupStatus() error

	// The object is only inserted if it wasn't there before.
	// The returned bool is true, if it was previously unknown.
	StoreObject(*Object) (bool, error)
	DeleteObject(string) error
	GetObject(string) (*Object, error)

	// Gather all the submissions for the object, extract the filenames
	// and the sources and store them in the object
	UpdateObject(string) error

	StoreSubmission(*Submission) error
	DeleteSubmission(string) error
	GetSubmission(string) (*Submission, error)

	// Stores a result in the database
	// (TODO: return generated Id)
	StoreResult(*Result) error

	// Gets a result by Id from the database
	GetResult(string) (*Result, error)

	StoreConfig(*Config) error
	GetConfig(string) (*Config, error)

	// -------------------------------- //
	//          Status Database         //
	// -------------------------------- //

	// Main objects for identification and rarely changing data, which is not kept
	// as time series. This does not automatically deduplicate.
	StoreMachine(machine *Machine) error
	StorePlanner(planner *Planner) error
	StoreService(service *Service) error

	// Select rows matching the given non-nil criteria. No more than limit (int)
	// results may be emitted.
	GetMachine(machine_uuid string) (*Machine, error)
	GetPlanner(machine_uuid, planner_uuid string) (*Planner, error)
	GetService(planner_uuid string, port uint16) (*Service, error)
	GetMachines(limit int) ([]*Machine, error)
	GetPlanners(machine_uuid string, limit int) ([]*Planner, error)
	GetServices(planner_uuid string, limit int) ([]*Service, error)

	// Update rows matching the given non-nil criteria
	UpdateMachine(machine *Machine) error
	UpdatePlanner(planner *Planner) error
	UpdateService(service *Service) error

	// Delete rows matching the given non-nil criteria
	DeleteMachine(machine_uuid string) error
	DeletePlanner(machine_uuid, planner_uuid string) error
	DeleteService(planner_uuid string, service_port uint16) error
	DeletePlanners(machine_uuid string) error
	DeleteServices(planner_uuid string) error

	// Store time series data. (Logs, system status, etc)
	StoreSystemStatus(status *SystemStatus) error
	StorePlannerLogs(planner_uuid string, logs []*LogEntry) error
	StoreServiceLogs(service_uuid string, logs []*LogEntry) error
	StoreServiceTask(service_uuid string, task *TaskEntry) error

	// Get stored time series data as identified by the corresponding
	// machine_uuid (system status), planner_uuid (planner logs),
	// service_uuid (service logs and tasks)
	// The first timestamp is the start time frame, the second the end time frame.
	// The last parameter (int) is the limit. No more than limit results may be
	// emitted.
	GetSystemStatus(machine_uuid string, startDate, endDate time.Time, limit int) ([]*SystemStatus, error)
	GetPlannerLogs(planner_uuid string, startDate, endDate time.Time, limit int) ([]*LogEntry, error)
	GetServiceLogs(service_uuid string, startDate, endDate time.Time, limit int) ([]*LogEntry, error)
	GetServiceTasks(service_uuid string, startDate, endDate time.Time, limit int) ([]*TaskEntry, error)

	// Ability to store and retrieve arbitrary configuration data
	KvSet(path, value string) error
	KvGet(path string) (string, error)
	KvDel(path string) error
}

type Object struct {
	SHA256      string   `json:"sha256"`
	SHA1        string   `json:"sha1"`
	MD5         string   `json:"md5"`
	MIME        string   `json:"mime"`
	Source      []string `json:"source"`
	ObjName     []string `json:"obj_name"`
	Submissions []string `json:"submissions"`
}

type Submission struct {
	Id      string    `json:"id"`
	SHA256  string    `json:"sha256"`
	UserId  string    `json:"user_id"`
	Source  string    `json:"source"`
	Date    time.Time `json:"date"`
	ObjName string    `json:"obj_name"`
	Tags    []string  `json:"tags"`
	Comment string    `json:"comment"`
}

type Result struct {
	Id                string    `json:"id"`
	SHA256            string    `json:"sha256"`
	SchemaVersion     string    `json:"schema_version"`
	UserId            string    `json:"user_id"`
	SourceId          []string  `json:"source_id"`
	SourceTag         []string  `json:"source_tag"`
	ServiceName       string    `json:"service_name"`
	ServiceVersion    string    `json:"service_version"`
	ServiceConfig     string    `json:"service_config"`
	ObjectCategory    []string  `json:"object_category"`
	ObjectType        string    `json:"object_type"`
	Results           string    `json:"results"`
	Tags              []string  `json:"tags"`
	StartedDateTime   time.Time `json:"started_date_time"`
	FinishedDateTime  time.Time `json:"finished_date_time"`
	WatchguardStatus  string    `json:"watchguard_status"`
	WatchguardLog     []string  `json:"watchguard_log"`
	WatchguardVersion string    `json:"watchguard_version"`
}

type Config struct {
	Path         string `json:"path"`
	FileContents string `json:"file_contents"`
}

type Machine struct {
	MachineUUID       string    `json:"machine_uuid"`
	Harddrives        string    `json:"harddrives"`         // saved as a json blob in the db
	NetworkInterfaces string    `json:"network_interfaces"` // saved as a json blob in the db
	FirstSeen         time.Time `json:"first_seen"`
	LastSeen          time.Time `json:"last_seen"` // saved in a separate table
}

type SystemStatus struct {
	MachineUUID string    `json:"machine_uuid"`
	CpuIOWait   uint64    `json:"cpu_iowait"`
	CpuIdle     uint64    `json:"cpu_idle"`
	CpuBusy     uint64    `json:"cpu_busy"`
	CpuTotal    uint64    `json:"cpu_total"`
	MemUsage    uint64    `json:"mem_usage"`
	MemMax      uint64    `json:"mem_max"`
	SwapUsage   uint64    `json:"swap_usage"`
	SwapMax     uint64    `json:"swap_max"`
	Loads1      float64   `json:"loads_1"` // System load as reported by sysinfo syscall
	Loads5      float64   `json:"loads_5"`
	Loads15     float64   `json:"loads_15"`
	Timestamp   time.Time `json:"timestamp"`
}

type Planner struct {
	MachineUUID   string    `json:"machine_uuid"`
	PlannerUUID   string    `json:"planner_uuid"`
	Name          string    `json:"name"`
	PID           uint64    `json:"pid"`
	IP            string    `json:"ip"`
	Port          uint16    `json:"port"`
	Configuration string    `json:"configuration"`
	FirstSeen     time.Time `json:"first_seen"`
	LastSeen      time.Time `json:"last_seen"` // saved in a separate table
	// Logs          []*LogEntry `json:"logs"`
	// Services      []*Service  `json:"services"`
}

type Service struct {
	PlannerUUID   string `json:"planner_uuid"`
	Port          uint16 `json:"port"`
	ServiceUUID   string `json:"service_uuid"`
	Name          string `json:"name"`
	Configuration string `json:"configuration"`
	// TaskHistory   []*TaskEntry `json:"task_history"`
	// Logs          []*LogEntry  `json:"logs"`
}

type LogEntry struct {
	Message   string
	Timestamp time.Time
}

type TaskEntry struct {
	Task      string
	Timestamp time.Time
}
