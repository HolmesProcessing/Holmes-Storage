package StorerCassandra

import (
	"errors"
	"regexp"
	"strings"
	"time"

	sg "github.com/HolmesProcessing/Holmes-Storage/storerGeneric"
	"github.com/gocql/gocql"
)

// general purpose constants
const (
	max_limit = 2147483647
)

// -------------------------------------------------------------------------- //
// Storage functions.
// -------------------------------------------------------------------------- //
const (
	query_insert_machine = `
    INSERT INTO machines (machine_uuid, harddrives, network_interfaces, first_seen)
    VALUES (?, ?, ?, ?);`
	query_insert_planner = `
    INSERT INTO planners (machine_uuid, planner_uuid, name, ip, port, configuration, first_seen)
    VALUES (?, ?, ?, ?, ?, ?, ?);`
	query_insert_service = `
    INSERT INTO services (planner_uuid, service_uuid, name, port, configuration)
    VALUES (?, ?, ?, ?, ?);`

	query_insert_machine_lastseen = `
    INSERT INTO machines_lastseen_ts (machine_uuid, last_seen)
    VALUES (?, ?)
    USING TTL 300;`
	query_insert_planner_lastseen = `
    INSERT INTO planners_lastseen_ts (planner_uuid, last_seen)
    VALUES (?, ?)
    USING TTL 300;`
)

func (this StorerCassandra) StoreMachine(m *sg.Machine) error {
	if _, err := gocql.ParseUUID(m.MachineUUID); err != nil {
		return err
	}
	err := this.StatusDB.Query(query_insert_machine, m.MachineUUID, m.Harddrives, m.NetworkInterfaces, m.FirstSeen).Exec()
	if err == nil {
		err = this.StatusDB.Query(query_insert_machine_lastseen, m.MachineUUID, m.LastSeen).Exec()
	}
	return err
}

func (this StorerCassandra) StorePlanner(p *sg.Planner) error {
	if _, err := gocql.ParseUUID(p.MachineUUID); err != nil {
		return err
	}
	if _, err := gocql.ParseUUID(p.PlannerUUID); err != nil {
		return err
	}
	err := this.StatusDB.Query(query_insert_planner, p.MachineUUID, p.PlannerUUID, p.Name, p.IP, int(p.Port), p.Configuration, p.FirstSeen).Exec()
	if err == nil {
		err = this.StatusDB.Query(query_insert_planner_lastseen, p.PlannerUUID, p.LastSeen).Exec()
	}
	return err
}

func (this StorerCassandra) StoreService(s *sg.Service) error {
	if _, err := gocql.ParseUUID(s.PlannerUUID); err != nil {
		return err
	}
	s.ServiceUUID = gocql.TimeUUID().String()
	return this.StatusDB.Query(query_insert_service, s.PlannerUUID, s.ServiceUUID, s.Name, s.Port, s.Configuration).Exec()
}

// -------------------------------------------------------------------------- //
// Update functions.
// -------------------------------------------------------------------------- //
const (
	query_update_machine = `
    UPDATE machines SET
      harddrives = ?,
      network_interfaces = ?
    WHERE machine_uuid = ?;`
	query_update_planner = `
    UPDATE planners SET
      name = ?,
      ip = ?,
      port = ?,
      configuration = ?
    WHERE machine_uuid = ? AND planner_uuid = ?;`
	query_update_service = `
    UPDATE services SET
      service_uuid = ?,
      name = ?,
      configuration = ?
    WHERE planner_uuid = ? AND port = ?;`
)

func (this StorerCassandra) UpdateMachine(m *sg.Machine) error {
	if _, err := gocql.ParseUUID(m.MachineUUID); err != nil {
		return err
	}
	err := this.StatusDB.Query(query_update_machine, m.Harddrives, m.NetworkInterfaces, m.MachineUUID).Exec()
	if err == nil {
		err = this.StatusDB.Query(query_insert_machine_lastseen, m.MachineUUID, m.LastSeen).Exec()
	}
	return err
}

func (this StorerCassandra) UpdatePlanner(p *sg.Planner) error {
	if _, err := gocql.ParseUUID(p.MachineUUID); err != nil {
		return err
	}
	if _, err := gocql.ParseUUID(p.PlannerUUID); err != nil {
		return err
	}
	err := this.StatusDB.Query(query_update_planner, p.Name, p.IP, p.Port, p.Configuration, p.MachineUUID, p.PlannerUUID).Exec()
	if err == nil {
		err = this.StatusDB.Query(query_insert_planner_lastseen, p.PlannerUUID, p.LastSeen).Exec()
	}
	return err
}

func (this StorerCassandra) UpdateService(s *sg.Service) error {
	if _, err := gocql.ParseUUID(s.PlannerUUID); err != nil {
		return err
	}
	return this.StatusDB.Query(query_update_service, s.ServiceUUID, s.Name, s.Configuration, s.PlannerUUID, int(s.Port)).Exec()
}

// -------------------------------------------------------------------------- //
// Getter functions.
// -------------------------------------------------------------------------- //
const (
	query_get_machine = `
    SELECT harddrives, network_interfaces, first_seen
    FROM machines
    WHERE machine_uuid = ?
    LIMIT 1;`
	query_get_machines = `
    SELECT machine_uuid, harddrives, network_interfaces, first_seen
    FROM machines
    LIMIT ?;`
	query_machine_lastseen = `
    SELECT last_seen
    FROM machines_lastseen_ts
    WHERE machine_uuid = ?
    LIMIT 1;`

	query_get_planner = `
    SELECT name, ip, port, configuration, first_seen
    FROM planners
    WHERE machine_uuid = ? AND planner_uuid = ?
    LIMIT 1;`
	query_get_planners = `
    SELECT planner_uuid, name, ip, port, configuration, first_seen
    FROM planners
    WHERE machine_uuid = ?
    LIMIT ?;`
	query_get_all_planners = `
    SELECT planner_uuid, name, ip, port, configuration, first_seen
    FROM planners
    LIMIT ?;`
	query_planner_lastseen = `
    SELECT last_seen
    FROM planners_lastseen_ts
    WHERE planner_uuid = ?
    LIMIT 1;`

	query_get_service = `
    SELECT service_uuid, name, configuration
    FROM services
    WHERE planner_uuid = ? AND port = ?
    LIMIT 1;`
	query_get_services = `
    SELECT service_uuid, name, port, configuration
    FROM services
    WHERE planner_uuid = ?
    LIMIT ?;`
)

func (this StorerCassandra) GetMachine(machine_uuid string) (*sg.Machine, error) {
	if _, err := gocql.ParseUUID(machine_uuid); err != nil {
		return nil, err
	}
	result := &sg.Machine{
		MachineUUID: machine_uuid,
	}
	err := this.StatusDB.Query(query_get_machine, machine_uuid).Scan(
		&result.Harddrives,
		&result.NetworkInterfaces,
		&result.FirstSeen,
	)
	if err == nil {
		err = this.StatusDB.Query(query_machine_lastseen, machine_uuid).Scan(&result.LastSeen)
	}
	return result, err
}

func (this StorerCassandra) GetMachines(limit int) ([]*sg.Machine, error) {
	if limit == -1 {
		limit = max_limit
	}
	iter := this.StatusDB.Query(query_get_machines, limit).Iter()
	count := iter.NumRows()
	results := make([]*sg.Machine, count)
	var (
		err2 error
	)
	for i := 0; i < count; i++ {
		results[i] = &sg.Machine{}
		ok := iter.Scan(
			&results[i].MachineUUID,
			&results[i].Harddrives,
			&results[i].NetworkInterfaces,
			&results[i].FirstSeen,
		)
		// err2 = this.StatusDB.Query(query_machine_lastseen, results[i].MachineUUID).Scan(&results[i].LastSeen)
		// if err2 != nil {
		// ok = false
		// }
		if !ok {
			break
		}
	}
	err := iter.Close()
	if err == nil {
		err = err2
	}
	return results, err
}

func (this StorerCassandra) GetPlanner(machine_uuid, planner_uuid string) (*sg.Planner, error) {
	_, err := gocql.ParseUUID(machine_uuid)
	if err != nil {
		return nil, err
	}
	_, err = gocql.ParseUUID(planner_uuid)
	if err != nil {
		return nil, err
	}

	result := &sg.Planner{
		MachineUUID: machine_uuid,
		PlannerUUID: planner_uuid,
	}
	var port int
	err = this.StatusDB.Query(query_get_planner, machine_uuid, planner_uuid).Scan(
		&result.Name,
		&result.IP,
		&port,
		&result.Configuration,
		&result.FirstSeen,
	)
	result.Port = uint16(port)
	return result, err
}

func (this StorerCassandra) GetPlanners(machine_uuid string, limit int) ([]*sg.Planner, error) {
	var (
		err  error
		iter *gocql.Iter
	)
	if limit == -1 {
		limit = max_limit
	}
	if machine_uuid == "" {
		iter = this.StatusDB.Query(query_get_all_planners, limit).Iter()
	} else {
		_, err = gocql.ParseUUID(machine_uuid)
		if err != nil {
			return nil, err
		}
		iter = this.StatusDB.Query(query_get_planners, machine_uuid, limit).Iter()
	}
	count := iter.NumRows()
	results := make([]*sg.Planner, count)
	var (
		port int
		err2 error
	)
	for i := 0; i < count; i++ {
		results[i] = &sg.Planner{MachineUUID: machine_uuid}
		ok := iter.Scan(
			&results[i].PlannerUUID,
			&results[i].Name,
			&results[i].IP,
			&port,
			&results[i].Configuration,
			&results[i].FirstSeen,
		)
		results[i].Port = uint16(port)
		err2 = this.StatusDB.Query(query_planner_lastseen, results[i].PlannerUUID).Scan(&results[i].LastSeen)
		if err2 != nil {
			ok = false
		}
		if !ok {
			break
		}
	}
	err = iter.Close()
	if err == nil {
		err = err2
	}
	return results, err
}

func (this StorerCassandra) GetService(planner_uuid string, port uint16) (*sg.Service, error) {
	_, err := gocql.ParseUUID(planner_uuid)
	if err != nil {
		return nil, err
	}
	result := &sg.Service{
		PlannerUUID: planner_uuid,
		Port:        port,
	}
	err = this.StatusDB.Query(query_get_service, planner_uuid, int(port)).Scan(
		&result.ServiceUUID,
		&result.Name,
		&result.Configuration,
	)
	return result, err
}

func (this StorerCassandra) GetServices(planner_uuid string, limit int) ([]*sg.Service, error) {
	_, err := gocql.ParseUUID(planner_uuid)
	if err != nil {
		return nil, err
	}
	if limit == -1 {
		limit = max_limit
	}
	iter := this.StatusDB.Query(query_get_services, planner_uuid, limit).Iter()
	count := iter.NumRows()
	results := make([]*sg.Service, count)
	var (
		port int
	)
	for i := 0; i < count; i++ {
		results[i] = &sg.Service{PlannerUUID: planner_uuid}
		ok := iter.Scan(
			&results[i].ServiceUUID,
			&results[i].Name,
			&port,
			&results[i].Configuration,
		)
		results[i].Port = uint16(port)
		if !ok {
			break
		}
	}
	err = iter.Close()
	return results, err
}

// -------------------------------------------------------------------------- //
// Time series storage functions.
// -------------------------------------------------------------------------- //
const (
	query_insert_systemstatus = `
    INSERT INTO machines_systemstatus_ts (
      machine_uuid,
      cpu_iowait, cpu_idle, cpu_busy, cpu_total,
      mem_usage, mem_max,
      swap_usage, swap_max,
      loads_1, loads_5, loads_15,
      timestamp
    )
    VALUES (?,   ?, ?, ?, ?,   ?, ?,   ?, ?,   ?, ?, ?,   ?)
    USING TTL 3600;`
	query_insert_plannerlog = `
    INSERT INTO planners_logs_ts (
      planner_uuid,
      timestamp,
      message
    )
    VALUES (?, ?, ?)
    USING TTL 3600;`
	query_insert_servicelog = `
    INSERT INTO service_logs_ts (
      service_uuid,
      timestamp,
      message
    )
    VALUES (?, ?, ?)
    USING TTL 3600;`
	query_insert_servicetask = `
    INSERT INTO service_tasks_ts (
      service_uuid,
      timestamp,
      task
    )
    VALUES (?, ?, ?)
    USING TTL 3600;`
)

func (this StorerCassandra) StoreSystemStatus(s *sg.SystemStatus) error {
	if _, err := gocql.ParseUUID(s.MachineUUID); err != nil {
		return err
	}
	return this.StatusDB.Query(query_insert_systemstatus,
		s.MachineUUID,
		s.CpuIOWait, s.CpuIdle, s.CpuBusy, s.CpuTotal,
		s.MemUsage, s.MemMax,
		s.SwapUsage, s.SwapMax,
		s.Loads1, s.Loads5, s.Loads15,
		s.Timestamp).Exec()
}

func (this StorerCassandra) StorePlannerLogs(planner_uuid string, logs []*sg.LogEntry) error {
	if _, err := gocql.ParseUUID(planner_uuid); err != nil {
		return err
	}
	for _, log := range logs {
		if err := this.StatusDB.Query(query_insert_plannerlog, planner_uuid, log.Timestamp, log.Message).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (this StorerCassandra) StoreServiceLogs(service_uuid string, logs []*sg.LogEntry) error {
	if _, err := gocql.ParseUUID(service_uuid); err != nil {
		return err
	}
	for _, log := range logs {
		if err := this.StatusDB.Query(query_insert_servicelog, service_uuid, log.Timestamp, log.Message).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (this StorerCassandra) StoreServiceTask(service_uuid string, task *sg.TaskEntry) error {
	if _, err := gocql.ParseUUID(service_uuid); err != nil {
		return err
	}
	return this.StatusDB.Query(query_insert_servicetask, service_uuid, task.Timestamp, task.Task).Exec()
}

// -------------------------------------------------------------------------- //
// Time series getter functions.
// -------------------------------------------------------------------------- //
const (
	query_get_systemstatus = `
    SELECT
      cpu_iowait, cpu_idle, cpu_busy, cpu_total,
      mem_usage, mem_max,
      swap_usage, swap_max,
      loads_1, loads_5, loads_15,
      timestamp
    FROM
      machines_systemstatus_ts
    WHERE
      machine_uuid = ? AND
      timestamp > ? AND
      timestamp < ?
    LIMIT ?;`
	query_get_plannerlogs = `
    SELECT
      timestamp, message
    FROM
      planners_logs_ts
    WHERE
      planner_uuid = ? AND
      timestamp > ? AND
      timestamp < ?
    LIMIT ?;`
	query_get_servicelogs = `
    SELECT
      timestamp, message
    FROM
      services_logs_ts
    WHERE
      service_uuid = ? AND
      timestamp > ? AND
      timestamp < ?
    LIMIT ?;`
	query_get_servicetasks = `
    SELECT
      timestamp, task
    FROM
      service_tasks_ts
    WHERE
      service_uuid = ? AND
      timestamp > ? AND
      timestamp < ?
    LIMIT ?;`
)

func (this StorerCassandra) GetSystemStatus(machine_uuid string, start, end time.Time, limit int) ([]*sg.SystemStatus, error) {
	if _, err := gocql.ParseUUID(machine_uuid); err != nil {
		return nil, err
	}
	if limit == -1 {
		limit = max_limit
	}
	_i_ := this.StatusDB.Query(query_get_systemstatus, machine_uuid, start, end, limit).Iter()
	n := _i_.NumRows()
	r := make([]*sg.SystemStatus, n)
	for i := 0; i < n; i++ {
		r[i] = &sg.SystemStatus{
			MachineUUID: machine_uuid,
		}
		if !_i_.Scan(
			&r[i].CpuIOWait, &r[i].CpuIdle, &r[i].CpuBusy, &r[i].CpuTotal,
			&r[i].MemUsage, &r[i].MemMax,
			&r[i].SwapUsage, &r[i].SwapMax,
			&r[i].Loads1, &r[i].Loads5, &r[i].Loads15,
			&r[i].Timestamp,
		) {
			break
		}
	}
	return r, _i_.Close()
}

func (this StorerCassandra) GetPlannerLogs(planner_uuid string, start, end time.Time, limit int) ([]*sg.LogEntry, error) {
	if _, err := gocql.ParseUUID(planner_uuid); err != nil {
		return nil, err
	}
	if limit == -1 {
		limit = max_limit
	}
	_i_ := this.StatusDB.Query(query_get_plannerlogs, planner_uuid, start, end, limit).Iter()
	n := _i_.NumRows()
	r := make([]*sg.LogEntry, n)
	for i := 0; i < n; i++ {
		r[i] = &sg.LogEntry{}
		if !_i_.Scan(
			&r[i].Timestamp,
			&r[i].Message,
		) {
			break
		}
	}
	return r, _i_.Close()
}

func (this StorerCassandra) GetServiceLogs(service_uuid string, start, end time.Time, limit int) ([]*sg.LogEntry, error) {
	if _, err := gocql.ParseUUID(service_uuid); err != nil {
		return nil, err
	}
	if limit == -1 {
		limit = max_limit
	}
	_i_ := this.StatusDB.Query(query_get_servicelogs, service_uuid, start, end, limit).Iter()
	n := _i_.NumRows()
	r := make([]*sg.LogEntry, n)
	for i := 0; i < n; i++ {
		r[i] = &sg.LogEntry{}
		if !_i_.Scan(
			&r[i].Timestamp,
			&r[i].Message,
		) {
			break
		}
	}
	return r, _i_.Close()
}

func (this StorerCassandra) GetServiceTasks(service_uuid string, start, end time.Time, limit int) ([]*sg.TaskEntry, error) {
	if _, err := gocql.ParseUUID(service_uuid); err != nil {
		return nil, err
	}
	if limit == -1 {
		limit = max_limit
	}
	_i_ := this.StatusDB.Query(query_get_servicetasks, service_uuid, start, end, limit).Iter()
	n := _i_.NumRows()
	r := make([]*sg.TaskEntry, n)
	for i := 0; i < n; i++ {
		r[i] = &sg.TaskEntry{}
		if !_i_.Scan(
			&r[i].Timestamp,
			&r[i].Task,
		) {
			break
		}
	}
	return r, _i_.Close()
}

// -------------------------------------------------------------------------- //
// Deletion functions.
// -------------------------------------------------------------------------- //
const (
	query_delete_machine = `
    DELETE FROM machines
    WHERE machine_uuid = ?;`
	query_delete_planner = `
    DELETE FROM planners
    WHERE machine_uuid = ? AND planner_uuid = ?;`
	query_delete_service = `
    DELETE FROM services
    WHERE planner_uuid = ? AND port = ?;`
	query_delete_planners_of_machine = `
    DELETE FROM planners
    WHERE machine_uuid = ?;`
	query_delete_services_of_planner = `
    DELETE FROM services
    WHERE planner_uuid = ?;`
)

func (this StorerCassandra) DeleteMachine(machine_uuid string) error {
	if _, err := gocql.ParseUUID(machine_uuid); err != nil {
		return err
	}
	return this.StatusDB.Query(query_delete_machine, machine_uuid).Exec()
}

func (this StorerCassandra) DeletePlanner(machine_uuid, planner_uuid string) error {
	if _, err := gocql.ParseUUID(machine_uuid); err != nil {
		return err
	}
	if _, err := gocql.ParseUUID(planner_uuid); err != nil {
		return err
	}
	return this.StatusDB.Query(query_delete_planner, machine_uuid, planner_uuid).Exec()
}

func (this StorerCassandra) DeleteService(planner_uuid string, service_port uint16) error {
	if _, err := gocql.ParseUUID(planner_uuid); err != nil {
		return err
	}
	return this.StatusDB.Query(query_delete_service, planner_uuid, int(service_port)).Exec()
}

func (this StorerCassandra) DeletePlanners(machine_uuid string) error {
	if _, err := gocql.ParseUUID(machine_uuid); err != nil {
		return err
	}
	return this.StatusDB.Query(query_delete_planners_of_machine, machine_uuid).Exec()
}

func (this StorerCassandra) DeleteServices(planner_uuid string) error {
	if _, err := gocql.ParseUUID(planner_uuid); err != nil {
		return err
	}
	return this.StatusDB.Query(query_delete_services_of_planner, planner_uuid).Exec()
}

// -------------------------------------------------------------------------- //
// Key value store functionality.
// -------------------------------------------------------------------------- //
var (
	// The value did change s
	ChangedValueError = errors.New("ChangedValueError")
)

const (
	query_kvstore_select = `
    SELECT key, value FROM kvstore
    WHERE parent=? AND id=?;`
	query_kvstore_insert = `
    INSERT INTO kvstore (parent, id, key, value)
    VALUES (?, ?, ?, ?)
    IF NOT EXISTS;`
	query_kvstore_update = `
    UPDATE kvstore SET value=?
    WHERE parent=? AND id=?
    IF value=?;`
	query_kvstore_delete = `
    DELETE FROM kvstore
    WHERE parent=? AND id=?;`

	query_kvIndex_select = `
    SELECT parent, id FROM kvstore_secondary
    WHERE path=?;`
	query_kvIndex_insert = `
    INSERT INTO kvstore_secondary (path, parent, id)
    VALUES (?, ?, uuid())
    IF NOT EXISTS;`
	query_kvIndex_update = `
    UPDATE kvstore_secondary SET parent=?
    WHERE path=?
    IF parent=?;`
	query_kvIndex_delete = `
    DELETE FROM kvstore_secondary
    WHERE path=?;`
)

// helper functions for kvs
func (this StorerCassandra) _kvs_select(parent, id gocql.UUID) (string, string, error) {
	var key, value string
	err := this.StatusDB.Query(query_kvstore_select, parent, id).Scan(&key, &value)
	return key, value, err
}

func (this StorerCassandra) _kvs_insert(parent, id gocql.UUID, key, value string) error {
	return this.StatusDB.Query(query_kvstore_insert, parent, id, key, value).Exec()
}

func (this StorerCassandra) _kvs_update(parent, id gocql.UUID, value, prev_value string) error {
	return this.StatusDB.Query(query_kvstore_update, value, parent, id, prev_value).Exec()
}

func (this StorerCassandra) _kvs_delete(parent, id gocql.UUID) error {
	return this.StatusDB.Query(query_kvstore_delete, parent, id).Exec()
}

// helper functions for kvsIndex
func (this StorerCassandra) _kvsIndex_select(path string) (gocql.UUID, gocql.UUID, error) {
	var parent, id gocql.UUID
	err := this.StatusDB.Query(query_kvIndex_select, path).Scan(&parent, &id)
	return parent, id, err
}

func (this StorerCassandra) _kvsIndex_insert(path string, parent gocql.UUID) error {
	return this.StatusDB.Query(query_kvIndex_insert, path, parent).Exec()
}

func (this StorerCassandra) _kvsIndex_update(path string, parent gocql.UUID) error {
	return this.StatusDB.Query(query_kvIndex_update, parent, path, parent).Exec()
}

func (this StorerCassandra) _kvsIndex_delete(path string) error {
	return this.StatusDB.Query(query_kvIndex_delete, path).Exec()
}

// helper functions for main functions
func (this StorerCassandra) _kvsIndex_createPath(path string) error {
	var (
		s          = 1
		key        = ""
		next_path  = ""
		parent, id gocql.UUID
		err        error
	)
	for {
		switch s {
		case 1: // start
			if path == "" {
				return nil
			}
			i := strings.Index(path, "/")
			if i == -1 {
				i = len(path)
				key = path[0:i]
				path = ""
			} else {
				key = path[0:i]
				path = path[i+1 : len(path)]
			}
			if len(next_path) > 0 {
				next_path = next_path + "/" + key
			} else {
				next_path = key
			}
			s = 2 // --> goto select path

		case 2: // select path
			if _, id, err = this._kvsIndex_select(next_path); err == gocql.ErrNotFound {
				s = 3 // --> goto insert path
			} else if err == nil {
				parent = id
				s = 1 // --> goto start
			} else {
				return err
			}

		case 3: // create path
			if err = this._kvsIndex_insert(next_path, parent); err != nil {
				return err
			}
			if _, id, err = this._kvsIndex_select(next_path); err != nil {
				return err
			}
			if err = this._kvs_insert(parent, id, key, ""); err != nil {
				return err
			}
			s = 2 // --> goto select
		}
	}
}

// Main abstracted functions
func (this StorerCassandra) KvSet(path, value string) error {
	var (
		err        error
		parent, id gocql.UUID
		prev_value string
	)
	// clean path
	path = re_clean_path.ReplaceAllString(strings.Trim(path, "/"), "/")

	// make sure the path exists
	for {
		if parent, id, err = this._kvsIndex_select(path); err == gocql.ErrNotFound {
			err = this._kvsIndex_createPath(path)
		} else if err == nil {
			break
		}
		if err != nil {
			return err
		}
	}

	// select our primary
	key := path[(strings.LastIndex(path, "/") + 1):len(path)]
	if _, prev_value, err = this._kvs_select(parent, id); err == gocql.ErrNotFound {
		return this._kvs_insert(parent, id, key, value)
	} else if err == nil {
		return this._kvs_update(parent, id, value, prev_value)
	} else {
		return err
	}
}

var (
	re_clean_path = regexp.MustCompile(`/+`)
)

func (this StorerCassandra) KvGet(path string) (string, error) {
	// clean path
	path = re_clean_path.ReplaceAllString(strings.Trim(path, "/"), "/")
	// find it
	var value string
	parent, id, err := this._kvsIndex_select(path)
	if err == nil {
		_, value, err = this._kvs_select(parent, id)
		if err == gocql.ErrNotFound {
			err = nil
		}
	}
	return value, err
}

func (this StorerCassandra) KvDel(path string) error {
	return this.KvSet(path, "")
}
