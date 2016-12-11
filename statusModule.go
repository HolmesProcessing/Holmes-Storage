package main

import (
	"github.com/julienschmidt/httprouter"
	types "github.com/ms-xy/Holmes-Planner-Monitor/go/msgtypes"
	"github.com/ms-xy/Holmes-Planner-Monitor/go/server"

	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Initialize this module, effectively launching goroutines to handle incoming
// and outgoing traffic.
func initStatusModule(httpBinding string) {
	router := &StatusRouter{
		machines: make(map[types.UUID]*SystemInformation),
	}
	server.ListenAndServe(httpBinding, router) // does not block

	httprouter := httprouter.New()
	httprouter.GET("/status/get_uuids", router.httpGetUuids)
	httprouter.GET("/status/get_plannernames", router.httpGetPlannerNames)
	httprouter.GET("/status/get_sysinfo/:uuid", router.httpGetSysinfo)
	go func() {
		fmt.Println(http.ListenAndServe(httpBinding, httprouter))
	}()

	router.mainLoop() // does block
	// TODO: unify the way functions block
}

// Data structures containing information about all planners and their services
// on a single machine, identified by its uuid.
// Planners are identified by the process ID that they inhabit. This way we can
// also keep track of planners that died and potentially restarted using a
// different PID shortly after (logs could potentially indicate this too).
// Similarily the services that accompany a planner are reported as children of
// that very planner, mapped by their respective port numbers.
type SystemInformation struct {
	SystemStatus  *types.SystemStatus
	NetworkStatus *types.NetworkStatus
	Planners      map[uint64]*PlannerInformation
}

type PlannerInformation struct {
	Name          string
	PID           uint64
	IP            net.IP
	Port          int
	Configuration string
	Logs          *LogBuffer
	Services      map[uint16]*ServiceInformation
}

type ServiceInformation struct {
	Configuration string
	Name          string
	Port          uint16
	Task          string
	Logs          *LogBuffer
}

// A simple log buffer, discards old messages if to make space for new ones.
// This is only a temporary solution, as in the end the database
// will be responsible for saving logs and tombstones in Cassandra
// make it a lot easier.
func NewDefaultLogBuffer() *LogBuffer {
	return NewLogBuffer(0x400) // 1024 lines
}
func NewLogBuffer(size int) *LogBuffer {
	return &LogBuffer{
		buffer:   make([]string, size),
		capacity: size,
		last:     -1,
		length:   0,
	}
}

type LogBuffer struct {
	sync.Mutex
	buffer   []string
	capacity int
	last     int
	length   int
}

func (this *LogBuffer) Append(msgs []string) {
	this.Lock()
	defer this.Unlock()
	for _, msg := range msgs {
		this.last = (this.last + 1) % this.capacity
		this.buffer[this.last] = msg
	}
	this.length += len(msgs)
	if this.length > this.capacity {
		this.length = this.capacity
	}
}

func (this *LogBuffer) GetLastN(n int) []string {
	if n > this.length {
		n = this.length
	}
	if n > this.last {
		// we have to copy ...
		// [xxxxxx|------xx]
		// 0      l      n
		// -> remainder = l-n
		// -> buffer[(capacity+remainder):]
		r := make([]string, n)
		oflen := this.capacity + (this.last - n)
		copy(r, this.buffer[oflen:])
		copy(r[oflen:], this.buffer[:this.last])
		return r
	}
	return this.buffer[this.last-n : this.last]
}

// The StatusRouter is the structure doing all the work in this module.
// Connected clients are stored in the sessions map to be able to perform
// search operations (e.g. collect orphaned sessions)
type StatusRouter struct {
	sync.Mutex
	machines map[types.UUID]*SystemInformation
}

// Loop for doing all the regular checks on sessions
// Including but not limited to checks if sessions are still to be considered
// alive and functioning or if an error should be issued
func (this *StatusRouter) mainLoop() {
	sessions := server.GetSessions()
	for range time.Tick(5 * time.Second) {
		go this.runLivelinessCheck(sessions)
	}
}

func (this *StatusRouter) runLivelinessCheck(sessions *server.SessionMap) {
	this.Lock()
	defer this.Unlock()
	// TODO: move this op into the database, cassandra has surely better search
	// capabilities than we do here
	// Each service should announce its presence at least every 30s
	// TODO: would it make sense to have this time frame configurable?
	tMinus30s := time.Now().Add(-30 * time.Second)
	sessions.ForEach(func(s *server.Session) {
		if s.LastSeen.Before(tMinus30s) {
			fmt.Println("==> malfunctioning node:", s.GetUuid().ToString(), s.Address)
		} else {
			fmt.Println("==> node alive:", s.GetUuid().ToString(), s.Address)
		}
	})
}

// Implement server.StatusRouter interface for our StatusRouter:
func (this *StatusRouter) RecvPlannerInfo(plannerinfo *types.PlannerInfo, session *server.Session, pid uint64) (cm *types.ControlMessage) {
	if plannerinfo.Disconnect {
		session.Close()
		delete(this.machines, *session.GetUuid())
		cm = &types.ControlMessage{AckDisconnect: true}

	} else if plannerinfo.Connect {
		var (
			si     *SystemInformation
			pi     *PlannerInformation
			exists bool
			uuid   = session.GetUuid()
		)

		if si, exists = this.machines[*uuid]; !exists {
			si = &SystemInformation{
				SystemStatus:  &types.SystemStatus{},
				NetworkStatus: &types.NetworkStatus{},
				Planners:      make(map[uint64]*PlannerInformation),
			}
			this.machines[*uuid] = si
		}

		if pi, exists = si.Planners[pid]; !exists {
			pi = &PlannerInformation{
				Name: plannerinfo.Name,
				PID:  pid,
				IP:   plannerinfo.ListenAddress.IP,
				Port: plannerinfo.ListenAddress.Port,
				Logs: NewDefaultLogBuffer(),
			}
			si.Planners[pid] = pi

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

		cm = &types.ControlMessage{AckConnect: true}
	}
	return
}

func (this *StatusRouter) RecvSystemStatus(systemstatus *types.SystemStatus, session *server.Session, pid uint64) (cm *types.ControlMessage) {
	if si, exists := this.machines[*session.GetUuid()]; exists {
		si.SystemStatus = systemstatus
	} else {
		warning.Println("Received SystemStatus for an unregistered planner:", session.GetUuid().ToString(), pid, session.Address)
	}
	return
}

func (this *StatusRouter) RecvNetworkStatus(networkstatus *types.NetworkStatus, session *server.Session, pid uint64) (cm *types.ControlMessage) {
	if si, exists := this.machines[*session.GetUuid()]; exists {
		si.NetworkStatus = networkstatus
	} else {
		warning.Println("Received NetworkStatus for an unregistered planner:", session.GetUuid().ToString(), pid, session.Address)
	}
	return
}

func (this *StatusRouter) RecvPlannerStatus(plannerstatus *types.PlannerStatus, session *server.Session, pid uint64) (cm *types.ControlMessage) {
	if si, exists := this.machines[*session.GetUuid()]; exists {
		if pi, exists := si.Planners[pid]; exists {
			pi.Configuration = plannerstatus.ConfigProfileName
			pi.Logs.Append(plannerstatus.Logs)
		}
	} else {
		warning.Println("Received PlannerStatus for an unregistered planner:", session.GetUuid().ToString(), pid, session.Address)
	}
	return
}

func (this *StatusRouter) RecvServiceStatus(servicestatus *types.ServiceStatus, session *server.Session, pid uint64) (cm *types.ControlMessage) {
	// TODO: implement
	return
}

func (this *StatusRouter) HandleError(err error, session *server.Session, pid uint64) {
	fmt.Println(err)
}

// ----------- functions for serving the web api ---------------------------- //

type StatusWebAPI_Session struct {
	Session *server.Session
	Info    *PlannerInformation
}

func (this *StatusRouter) httpGetPlannerNames(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sessions := server.GetSessions()

	i := 0
	size := sessions.Size()
	names_map := make(map[string]bool)
	names := make([]string, size)

	sessions.ForEach(func(session *server.Session) {
		si := this.machines[*session.GetUuid()]
		for _, pi := range si.Planners {
			if _, exists := names_map[pi.Name]; !exists {
				names_map[pi.Name] = true
				if i < size {
					names[i] = pi.Name
					i++
				} else {
					names = append(names, pi.Name)
				}
			}
		}
	})

	httpSendJson(w, names)
}

func (this *StatusRouter) httpGetUuids(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sessions := server.GetSessions()

	i := 0
	size := sessions.Size()
	uuids := make([]string, size)

	sessions.ForEach(func(session *server.Session) {
		uuid := session.GetUuid()
		if i < size {
			uuids[i] = uuid.ToString()
			i++
		} else {
			uuids = append(uuids, uuid.ToString())
		}
	})

	httpSendJson(w, uuids)
}

func (this *StatusRouter) httpGetSysinfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sessions := server.GetSessions()

	if uuid, err := types.UUIDFromString(ps.ByName("uuid")); err == nil {

		if session, exists := sessions.GetByUuid(uuid); exists {
			si := this.machines[*session.GetUuid()]
			httpSendJson(w, si.SystemStatus)

		} else {
			http.Error(w, "unknown uuid: "+uuid.ToString(), 404)
		}

	} else {
		http.Error(w, "invalid uuid: "+err.Error(), 404)
	}
}

func httpSendJson(w http.ResponseWriter, data interface{}) {
	json, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		w.Header().Set("Content-Type", "text/json")
		w.Write(json)
	}
}
