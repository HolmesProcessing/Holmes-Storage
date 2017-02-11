package Status

import (
	"github.com/julienschmidt/httprouter"

	"github.com/ms-xy/Holmes-Planner-Monitor/go/msgtypes"
	"github.com/ms-xy/Holmes-Planner-Monitor/go/server"

	"github.com/gocql/gocql"

	"encoding/json"
	"net/http"
	"reflect"
	"time"
)

func httpSendJson(w http.ResponseWriter, data interface{}) {
	json, err := json.Marshal(data)
	if err != nil {
		warning.Println(err, data, reflect.TypeOf(data))
		http.Error(w, err.Error(), 500)
	} else {
		w.Header().Set("Content-Type", "text/json")
		w.Write(json)
	}
}

// Functions to serve the Web-API:
// -------------------------------

func (this *Router) HttpGetPlanners(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// sessions := server.GetSessions()

	// If a uuid is specified grab only those planners.
	// Else grab all possible planner names.
	if machine_uuid := ps.ByName("machine_uuid"); machine_uuid != "" {
		if _, err := gocql.ParseUUID(machine_uuid); err == nil {
			if planners, err := this.db.GetPlanners(machine_uuid, -1); err == nil {
				httpSendJson(w, planners) // TODO
			} else {
				http.Error(w, "No planners found for machine: "+machine_uuid, 404)
			}
		} else {
			http.Error(w, "invalid machine_uuid: "+err.Error(), 400)
		}

	} else {
		if planners, err := this.db.GetPlanners("", -1); err != nil {
			httpSendJson(w, planners)
		} else {
			http.Error(w, "Error occured during fetching planners: "+err.Error(), 500)
		}
	}
}

func (this *Router) HttpGetNetinfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if machine_uuid, err := msgtypes.UUIDFromString(ps.ByName("machine_uuid")); err == nil {
		if machine, err := this.db.GetMachine(machine_uuid.ToString()); err == nil {
			httpSendJson(w, machine.NetworkInterfaces)
		} else {
			http.Error(w, "error fetching machine info from db: "+machine_uuid.ToString(), 404)
		}
	} else {
		http.Error(w, "invalid machine_uuid: "+err.Error(), 400)
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

		start := time.Time{}
		end := time.Now()

		if systemstatus, err := this.db.GetSystemStatus(machine_uuid.ToString(), start, end, 1); err == nil {
			httpSendJson(w, systemstatus)
		} else {
			http.Error(w, "error fetching systemstatus from db: "+err.Error(), 404)
		}

	} else {
		http.Error(w, "invalid machine_uuid: "+err.Error(), 404)
	}
}
