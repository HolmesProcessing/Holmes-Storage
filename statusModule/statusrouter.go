package Status

import (
	"github.com/ms-xy/Holmes-Planner-Monitor/go/msgtypes"
	"github.com/ms-xy/Holmes-Planner-Monitor/go/server"

	"fmt"
	"sync"
	"time"
)

func NewRouter() *Router {
	return &Router{
		machines: make(map[msgtypes.UUID]*SystemInformation),
	}
}

// The Router is the structure doing all the work in this module.
// Connected clients are stored in the sessions map to be able to perform
// search operations (e.g. collect orphaned sessions).
type Router struct {
	sync.Mutex
	machines map[msgtypes.UUID]*SystemInformation
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
func (this *Router) RecvPlannerInfo(plannerinfo *msgtypes.PlannerInfo, session *server.Session, pid uint64) (cm *msgtypes.ControlMessage) {
	if plannerinfo.Disconnect {
		// Clean up any remaining references after a disconnect. If there are no
		// remaining planners on the machine, remove the machine reference.
		session.Close()
		systeminformation, exists := this.machines[*session.GetMachineUuid()]
		if exists {
			delete(systeminformation.Planners, session.GetPID())
			if len(systeminformation.Planners) == 0 {
				delete(this.machines, *session.GetMachineUuid())
			}
		}
		cm = &msgtypes.ControlMessage{AckDisconnect: true}

	} else if plannerinfo.Connect {
		var (
			si           *SystemInformation
			pi           *PlannerInformation
			exists       bool
			machine_uuid = session.GetMachineUuid()
		)

		if si, exists = this.machines[*machine_uuid]; !exists {
			si = &SystemInformation{
				SystemStatus:  &msgtypes.SystemStatus{},
				NetworkStatus: &msgtypes.NetworkStatus{},
				Planners:      make(map[uint64]*PlannerInformation),
			}
			this.machines[*machine_uuid] = si
		}

		if pi, exists = si.Planners[session.GetPID()]; !exists {
			pi = &PlannerInformation{
				Name: plannerinfo.Name,
				PID:  session.GetPID(),
				IP:   plannerinfo.ListenAddress.IP,
				Port: plannerinfo.ListenAddress.Port,
				Logs: NewDefaultLogBuffer(),
			}
			si.Planners[session.GetPID()] = pi

		} else {
			if plannerinfo.Name != "" {
				pi.Name = plannerinfo.Name
			}
			if plannerinfo.ListenAddress.IP != nil {
				pi.IP = plannerinfo.ListenAddress.IP
			}
			if plannerinfo.ListenAddress.Port > 0 {
				pi.Port = plannerinfo.ListenAddress.Port
			}
		}

		cm = &msgtypes.ControlMessage{AckConnect: true}
	}
	return
}

func (this *Router) RecvSystemStatus(systemstatus *msgtypes.SystemStatus, session *server.Session, pid uint64) (cm *msgtypes.ControlMessage) {
	if si, exists := this.machines[*session.GetMachineUuid()]; exists {
		si.SystemStatus = systemstatus
	} else {
		warning.Println("Received SystemStatus for an unregistered planner:", session.GetUuid().ToString(), pid, session.Address)
	}
	return
}

func (this *Router) RecvNetworkStatus(networkstatus *msgtypes.NetworkStatus, session *server.Session, pid uint64) (cm *msgtypes.ControlMessage) {
	if si, exists := this.machines[*session.GetMachineUuid()]; exists {
		si.NetworkStatus = networkstatus
	} else {
		warning.Println("Received NetworkStatus for an unregistered planner:", session.GetUuid().ToString(), pid, session.Address)
	}
	return
}

func (this *Router) RecvPlannerStatus(plannerstatus *msgtypes.PlannerStatus, session *server.Session, pid uint64) (cm *msgtypes.ControlMessage) {
	if si, exists := this.machines[*session.GetMachineUuid()]; exists {
		if pi, exists := si.Planners[pid]; exists {
			pi.Configuration = plannerstatus.ConfigProfileName
			pi.Logs.Append(plannerstatus.Logs)
		}
	} else {
		warning.Println("Received PlannerStatus for an unregistered planner:", session.GetUuid().ToString(), pid, session.Address)
	}
	return
}

func (this *Router) RecvServiceStatus(servicestatus *msgtypes.ServiceStatus, session *server.Session, pid uint64) (cm *msgtypes.ControlMessage) {
	// TODO: implement
	return
}

func (this *Router) HandleError(err error, session *server.Session, pid uint64) {
	// TODO: better handling
	fmt.Println(err)
}
