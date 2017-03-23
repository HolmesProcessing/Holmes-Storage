package storerMongoDB

import (
	"errors"
	"time"

	sg "github.com/HolmesProcessing/Holmes-Storage/storerGeneric"
)

func (s StorerMongoDB) CreateDBStatus(c []*sg.DBConnector) error {
	// Nothing to do here, since mongo creates the database on the fly
	return nil
}

func (s StorerMongoDB) InitializeStatus(c []*sg.DBConnector) (sg.Storer, error) {
	return nil, errors.New("MongoDB is not yet supported by the status module.")
}

func (s StorerMongoDB) SetupStatus() error {
	return errors.New("MongoDB is not yet supported by the status module.")
}

// Storage functions
func (s StorerMongoDB) StoreMachine(machine *sg.Machine) error {
	return nil
}

func (s StorerMongoDB) StorePlanner(planner *sg.Planner) error {
	return nil
}

func (s StorerMongoDB) StoreService(service *sg.Service) error {
	return nil
}

// Update functions
func (s StorerMongoDB) UpdateMachine(machine *sg.Machine) error {
	return nil
}

func (s StorerMongoDB) UpdatePlanner(planner *sg.Planner) error {
	return nil
}

func (s StorerMongoDB) UpdateService(service *sg.Service) error {
	return nil
}

// Getter functions
func (s StorerMongoDB) GetMachine(machine_uuid string) (*sg.Machine, error) {
	return nil, nil
}

func (s StorerMongoDB) GetPlanner(machine_uuid, planner_uuid string) (*sg.Planner, error) {
	return nil, nil
}

func (s StorerMongoDB) GetService(service_uri string) (*sg.Service, error) {
	return nil, nil
}

func (s StorerMongoDB) GetMachines(limit int) ([]*sg.Machine, error) {
	return nil, nil
}

func (s StorerMongoDB) GetPlanners(machine_uuid string, limit int) ([]*sg.Planner, error) {
	return nil, nil
}

func (s StorerMongoDB) GetServices(limit int) ([]*sg.Service, error) {
	return nil, nil
}

// Deletion functions
func (s StorerMongoDB) DeleteMachine(machine_uuid string) error {
	return nil
}

func (s StorerMongoDB) DeletePlanner(machine_uuid, planner_uuid string) error {
	return nil
}

func (s StorerMongoDB) DeleteService(planner_uuid string, service_port uint16) error {
	return nil
}

func (s StorerMongoDB) DeletePlanners(machine_uuid string) error {
	return nil
}

func (s StorerMongoDB) DeleteServices(planner_uuid string) error {
	return nil
}

// Time series storage functions
func (s StorerMongoDB) StoreSystemStatus(status *sg.SystemStatus) error {
	return nil
}
func (s StorerMongoDB) StorePlannerLogs(planner_uuid string, logs []*sg.LogEntry) error {
	return nil
}

func (s StorerMongoDB) StoreServiceLogs(service_uuid string, logs []*sg.LogEntry) error {
	return nil
}

func (s StorerMongoDB) StoreServiceTask(service_uuid string, task *sg.TaskEntry) error {
	return nil
}

// Time series getter functions
func (s StorerMongoDB) GetSystemStatus(machine_uuid string, startDate, endDate time.Time, limit int) ([]*sg.SystemStatus, error) {
	return nil, nil
}

func (s StorerMongoDB) GetPlannerLogs(planner_uuid string, startDate, endDate time.Time, limit int) ([]*sg.LogEntry, error) {
	return nil, nil
}

func (s StorerMongoDB) GetServiceLogs(service_uuid string, startDate, endDate time.Time, limit int) ([]*sg.LogEntry, error) {
	return nil, nil
}

func (s StorerMongoDB) GetServiceTasks(service_uuid string, startDate, endDate time.Time, limit int) ([]*sg.TaskEntry, error) {
	return nil, nil
}

// KV storage functions
func (s StorerMongoDB) KvSet(path, value string) error {
	return nil
}
func (s StorerMongoDB) KvGet(path string) (string, error) {
	return "", nil
}
func (s StorerMongoDB) KvDel(path string) error {
	return nil
}
