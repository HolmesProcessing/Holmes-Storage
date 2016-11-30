package main

import (
	types "github.com/ms-xy/Holmes-Planner-Monitor/go/msgtypes"
	"github.com/ms-xy/Holmes-Planner-Monitor/go/server"

	"fmt"
	"net"
	"sync"
	"time"
)

// Initialize this module, effectively launching goroutines to handle incoming
// and outgoing traffic.
func initStatusModule(httpBinding string) {
	router := &StatusRouter{
		planners: make(map[uint64]*PlannerInformation),
	}
	server.ListenAndServe(httpBinding, router)
	router.mainLoop(server.GetSessions())
}

// Data structures containing information about a planner
type PlannerInformation struct {
	IP            net.IP
	Port          int
	Configuration string
	Logs          *LogBuffer
	Status        struct {
		System struct {
			Uptime      time.Time
			CPULoad     float64
			MemoryUsage uint64
			MemoryMax   uint64
			DiskSpace   []*DiskSpaceInformation
			Load1s      float64
			Load5s      float64
			Load15s     float64
		}
		Network struct {
			// TODO determine layout - probably best place to do that is the protobuf
		}
		Services []*ServiceInformation
	}
}

type DiskSpaceInformation struct {
	Identifier string
	Used       uint64
	Total      uint64
}

type ServiceInformation struct {
	Configuration string
	Name          string
	IP            uint16
	Task          string
	Logs          *LogBuffer
}

// A simple log buffer, discards old messages if to make space for new ones
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
	planners map[uint64]*PlannerInformation
}

// Loop for doing all the regular checks on sessions
// Including but not limited to checks if sessions are still to be considered
// alive and functioning or if an error should be issued
func (this *StatusRouter) mainLoop(sessions *server.SessionMap) {
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
			fmt.Println("==> malfunctioning node:", s.GetID(), s.GetAddress())
		} else {
			fmt.Println("==> node alive:", s.GetID(), s.GetAddress())
		}
	})
}

// Implement server.StatusRouter interface for our StatusRouter:
func (this *StatusRouter) RecvPlannerInfo(plannerinfo *types.PlannerInfo, client *server.Session, isnew bool) *types.ControlMessage {
	if plannerinfo.Disconnect {
		client.Close()
		return &types.ControlMessage{AckDisconnect: true}

	} else if plannerinfo.Connect {
		// TODO: any special action for an entirely new client?

	}
	return &types.ControlMessage{AckConnect: true}
}

func (this *StatusRouter) RecvSystemStatus(systemstatus *types.SystemStatus, client *server.Session, isnew bool) *types.ControlMessage {
	fmt.Println(systemstatus)
	return nil
}

func (this *StatusRouter) RecvNetworkStatus(networkstatus *types.NetworkStatus, client *server.Session, isnew bool) *types.ControlMessage {
	fmt.Println(networkstatus)
	return nil
}

func (this *StatusRouter) RecvPlannerStatus(plannerstatus *types.PlannerStatus, client *server.Session, isnew bool) *types.ControlMessage {
	fmt.Println(plannerstatus)
	return nil
}

func (this *StatusRouter) RecvServiceStatus(servicestatus *types.ServiceStatus, client *server.Session, isnew bool) *types.ControlMessage {
	fmt.Println(servicestatus)
	return nil
}

func (this *StatusRouter) HandleError(err error, client *server.Session, isnew bool) {
	fmt.Println(err)
}
