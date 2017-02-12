package Status

import (
	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"
	"github.com/ms-xy/Holmes-Planner-Monitor/go/msgtypes"
	"github.com/ms-xy/Holmes-Planner-Monitor/go/server"

	"github.com/gocql/gocql"

	"encoding/json"
	"fmt"
	_runtime_debug "runtime/debug"
	"sync"
	"time"
)

func NewRouter(mainStorer storerGeneric.Storer) *Router {
	return &Router{
		db: mainStorer,
	}
}

// The Router is the structure doing all the work in this module.
// Connected clients are stored in the sessions map to be able to perform
// search operations (e.g. collect orphaned sessions).
type Router struct {
	sync.Mutex
	db storerGeneric.Storer
}

// Loop for doing all the regular checks on sessions
// Including but not limited to checks if sessions are still to be considered
// alive and functioning or if an error should be issued
func (this *Router) Run() {
	sessions := server.GetSessions()
	for range time.Tick(5 * time.Second) {
		go this.runLivelinessCheck(sessions)
	}
}

func (this *Router) runLivelinessCheck(sessions *server.SessionMap) {
	this.Lock()
	defer this.Unlock()
	// TODO: move this op into the database, cassandra has surely better search
	// capabilities than we do here
	// Each service should announce its presence at least every 30s
	// TODO: would it make sense to have this time frame configurable?
	tMinus30s := time.Now().Add(-30 * time.Second)
	sessions.ForEachSession(func(s *server.Session) {
		if s.LastSeen.Before(tMinus30s) {
			fmt.Println("==> malfunctioning node:", s.GetUuid().ToString(), s.Address)
		} else {
			fmt.Println("==> node alive:", s.GetUuid().ToString(), s.Address)
		}
	})
}

// ----------------------------------------
// Implement server.Router interface:
// ----------------------------------------

// PlannerInfo objects are received when a planner signs in or out. This
// mechanism allows us to keep track of malfunctioning planners without
// accidentally mixing it up with correctly disconnected planners.
func (this *Router) RecvPlannerInfo(plannerinfo *msgtypes.PlannerInfo, session *server.Session) (cm *msgtypes.ControlMessage) {

	var (
		machine_uuid = session.GetMachineUuid().ToString()
		planner_uuid = session.GetUuid().ToString()
	)
	planner, err := this.db.GetPlanner(machine_uuid, planner_uuid)
	if err == gocql.ErrNotFound {
		planner = nil
	} else if err != nil {
		return this.HandleError(err, session)
	}

	if plannerinfo.Disconnect {
		// Clean up any remaining references after a disconnect. If there are no
		// remaining planners on the machine, remove the machine reference.
		defer session.Close()

		if planner != nil {
			err := this.db.DeletePlanner(machine_uuid, planner_uuid)
			if err != nil && err != gocql.ErrNotFound {
				return this.HandleError(err, session)
			}
		}

		// TODO: evaluate if it really is smart to execute a delete here, might
		// overlap with a machine creation below
		_, err = this.db.GetPlanners(machine_uuid, 1)
		if err == gocql.ErrNotFound {
			this.db.DeleteMachine(machine_uuid)
		} else if err != nil {
			return this.HandleError(err, session)
		}

		cm = &msgtypes.ControlMessage{AckDisconnect: true}

	} else if plannerinfo.Connect {
		now := time.Now()

		// Create a new machine, if previously unknown
		_, err := this.db.GetPlanners(machine_uuid, 1)
		if err == gocql.ErrNotFound {
			err = this.db.StoreMachine(&storerGeneric.Machine{
				MachineUUID: machine_uuid,
				FirstSeen:   now,
				LastSeen:    now,
			})
			if err != nil {
				return this.HandleError(err, session)
			}
		}
		if err != nil {
			return this.HandleError(err, session)
		}

		// Create a new planner, if the uuid combination is unknown
		if planner == nil {
			var (
				ipaddr      = ""
				uint16 port = 0
			)
			if plannerinfo.ListenAddress != nil {
				ipaddr = plannerinfo.ListenAddress.IP.String()
				port = uint16(plannerinfo.ListenAddress.Port)
			}
			planner = &storerGeneric.Planner{
				MachineUUID: machine_uuid,
				PlannerUUID: planner_uuid,
				Name:        plannerinfo.Name,
				IP:          ipaddr,
				Port:        port,
				FirstSeen:   now,
				LastSeen:    now,
			}
			info.Println("New Planner registered: " + machine_uuid + "//" + planner.PlannerUUID)
			err := this.db.StorePlanner(planner)
			if err != nil {
				warning.Println("Error storing new planner in the database")
				return this.HandleError(err, session)
			}

		} else {
			// Update known planner otherwise
			if plannerinfo.Name != "" {
				planner.Name = plannerinfo.Name
			}
			if plannerinfo.ListenAddress.IP != nil {
				planner.IP = plannerinfo.ListenAddress.IP.String()
			}
			if plannerinfo.ListenAddress.Port > 0 {
				planner.Port = uint16(plannerinfo.ListenAddress.Port)
			}
			// Save changes back to the database
			this.db.UpdatePlanner(planner)
		}

		uuid := &msgtypes.UUID{}
		err = uuid.FromString(planner.PlannerUUID)
		if err != nil {
			return this.HandleError(err, session)
		}

		cm = &msgtypes.ControlMessage{AckConnect: true, UUID: uuid}
	}
	return
}

func (this *Router) RecvSystemStatus(s *msgtypes.SystemStatus, session *server.Session) (cm *msgtypes.ControlMessage) {
	machine_uuid := session.GetMachineUuid().ToString()
	if _, err := this.db.GetMachine(machine_uuid); err == gocql.ErrNotFound {
		warning.Println("Received SystemStatus for an unregistered machine (" + machine_uuid + ") from " + session.Address.String())
	} else if err != nil {
		return this.HandleError(err, session)
	} else {
		err := this.db.StoreSystemStatus(&storerGeneric.SystemStatus{
			MachineUUID: machine_uuid,
			CpuIOWait:   s.CpuIOWait,
			CpuIdle:     s.CpuIdle,
			CpuBusy:     s.CpuBusy,
			CpuTotal:    s.CpuTotal,
			MemUsage:    s.MemoryUsage,
			MemMax:      s.MemoryMax,
			SwapUsage:   s.SwapUsage,
			SwapMax:     s.SwapMax,
			Loads1:      s.Loads1,
			Loads5:      s.Loads5,
			Loads15:     s.Loads15,
			Timestamp:   time.Now(),
		})
		if err != nil {
			return this.HandleError(err, session)
		}
	}
	return
}

func (this *Router) RecvNetworkStatus(networkstatus *msgtypes.NetworkStatus, session *server.Session) (cm *msgtypes.ControlMessage) {
	var (
		machine_uuid = session.GetMachineUuid().ToString()
	)
	if machine, err := this.db.GetMachine(machine_uuid); err == gocql.ErrNotFound {
		warning.Println("Received NetworkStatus for an unregistered machine (" + machine_uuid + ") from: " + session.Address.String())
	} else if err != nil {
		return this.HandleError(err, session)
	} else {
		// previously unmarshalled, ignore error
		// TODO: either undo previous unmarshalling, or make db representation match
		// the struct more, so that unmarshalling makes sense
		bytes, _ := json.Marshal(networkstatus.Interfaces)
		machine.NetworkInterfaces = string(bytes)
		this.db.UpdateMachine(machine)
	}
	return
}

func (this *Router) RecvPlannerStatus(plannerstatus *msgtypes.PlannerStatus, session *server.Session) (cm *msgtypes.ControlMessage) {
	var (
		machine_uuid = session.GetMachineUuid().ToString()
		planner_uuid = session.GetUuid().ToString()
	)
	if planner, err := this.db.GetPlanner(machine_uuid, planner_uuid); err == gocql.ErrNotFound {
		warning.Println("Received PlannerStatus for an unregistered planner (" + planner_uuid + ") from: " + session.Address.String())
	} else if err != nil {
		return this.HandleError(err, session)
	} else {
		if plannerstatus.ConfigProfileName != "" {
			planner.Configuration = plannerstatus.ConfigProfileName
		}
		if plannerstatus.Logs != nil && len(plannerstatus.Logs) > 0 {
			logs := make([]*storerGeneric.LogEntry, len(plannerstatus.Logs))
			now := time.Now()
			for i, logmsg := range plannerstatus.Logs {
				logs[i] = &storerGeneric.LogEntry{
					Message:   logmsg,
					Timestamp: now, // TODO somehow get real time of the log message
				}
			}
			this.db.StorePlannerLogs(planner_uuid, logs)
		}
	}
	return
}

func (this *Router) RecvServiceStatus(sstat *msgtypes.ServiceStatus, session *server.Session) (cm *msgtypes.ControlMessage) {
	var (
		machine_uuid = session.GetMachineUuid().ToString()
		planner_uuid = session.GetUuid().ToString()
		service_port = sstat.Port
	)
	if _, err := this.db.GetPlanner(machine_uuid, planner_uuid); err == gocql.ErrNotFound {
		warning.Println("Received ServiceStatus for an unregistered planner (" + planner_uuid + ") from: " + session.Address.String())
	} else if err != nil {
		this.HandleError(err, session)
	} else {
		if service, err := this.db.GetService(planner_uuid, service_port); err == gocql.ErrNotFound {
			service = &storerGeneric.Service{
				PlannerUUID:   planner_uuid,
				Port:          service_port,
				ServiceUUID:   gocql.TimeUUID().String(),
				Name:          sstat.Name,
				Configuration: sstat.ConfigProfileName,
			}
			err = this.db.StoreService(service)
			if err != nil {
				return this.HandleError(err, session)
			}
		} else if err != nil {
			return this.HandleError(err, session)
		} else {
			if sstat.Name != "" {
				service.Name = sstat.Name
			}
			if sstat.ConfigProfileName != "" {
				service.Configuration = sstat.ConfigProfileName
			}
			err = this.db.UpdateService(service)
			if err != nil {
				return this.HandleError(err, session)
			}
		}
	}
	return
}

func (this *Router) HandleError(err error, session *server.Session) (cm *msgtypes.ControlMessage) {
	// TODO: better handling
	warning.Println(err)
	_runtime_debug.PrintStack()
	return
}
