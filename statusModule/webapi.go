package Status

import (
	"github.com/julienschmidt/httprouter"

	"github.com/ms-xy/Holmes-Planner-Monitor/go/msgtypes"
	"github.com/ms-xy/Holmes-Planner-Monitor/go/server"

	"encoding/json"
	"net/http"
)

func httpSendJson(w http.ResponseWriter, data interface{}) {
	json, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		w.Header().Set("Content-Type", "text/json")
		w.Write(json)
	}
}

// Functions to serve the Web-API:
// -------------------------------

func (this *Router) HttpGetPlanners(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sessions := server.GetSessions()

	// Grab only those planners running on the specified machine.
	// Else grab all but only their names.
	if machine_uuid, err := msgtypes.UUIDFromString(ps.ByName("machine_uuid")); err == nil {
		if si, exists := this.machines[*machine_uuid]; exists {
			httpSendJson(w, si.Planners)
		} else {
			http.Error(w, "unknown machine_uuid: "+machine_uuid.ToString(), 404)
		}

	} else {
		// This is the maximum upper boundary of unique planner names, which is only
		// ever touched if no single planner name is taken twice (unlikely at best).
		i := 0
		size := sessions.SizeSessions()
		names_map := make(map[string]bool, size)
		names := make([]string, size)
		for _, si := range this.machines {
			for _, pi := range si.Planners {
				if _, exists := names_map[pi.Name]; !exists {
					names_map[pi.Name] = true
					names[i] = pi.Name
					i++
				}
			}
		}
		httpSendJson(w, names)
	}
}

func (this *Router) HttpGetMachineUuids(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sessions := server.GetSessions()

	i := 0
	size := sessions.SizeMachines()
	machine_uuids := make([]string, size)

	sessions.ForEachMachine(func(machine_uuid *msgtypes.UUID, planners map[uint64]*server.Session) {
		if i < size {
			machine_uuids[i] = machine_uuid.ToString()
			i++
		} else {
			machine_uuids = append(machine_uuids, machine_uuid.ToString())
		}
	})

	httpSendJson(w, machine_uuids)
}

func (this *Router) HttpGetSysinfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if machine_uuid, err := msgtypes.UUIDFromString(ps.ByName("machine_uuid")); err == nil {

		if si, exists := this.machines[*machine_uuid]; exists {
			httpSendJson(w, si.SystemStatus)

		} else {
			http.Error(w, "unknown machine_uuid: "+machine_uuid.ToString(), 404)
		}

	} else {
		http.Error(w, "invalid machine_uuid: "+err.Error(), 404)
	}
}
