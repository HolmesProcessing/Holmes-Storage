package main

import (
	"github.com/julienschmidt/httprouter"

	"github.com/ms-xy/Holmes-Planner-Monitor/go/server"

	"github.com/HolmesProcessing/Holmes-Storage/statusModule"
	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"

	"fmt"
	"log"
	"net/http"
)

// Initialize this module, effectively launching goroutines to handle incoming
// and outgoing traffic.
// *This function blocks.*
func initStatusModule(httpBinding string, mainStorer storerGeneric.Storer, warning, info, debug *log.Logger) {
	Status.InitLogging(warning, info, debug)

	router := Status.NewRouter(mainStorer)
	server.ListenAndServe(httpBinding, router) // does not block

	httprouter := httprouter.New()
	httprouter.GET("/status/get_machines", router.HttpGetMachineUuids)
	httprouter.GET("/status/get_netinfo/:machine_uuid", router.HttpGetNetinfo)
	httprouter.GET("/status/get_planners/:machine_uuid", router.HttpGetPlanners)
	httprouter.GET("/status/get_sysinfo/:machine_uuid/:limit", router.HttpGetSysinfo)
	httprouter.GET("/kv/get/*path", router.HttpKvGet)
	httprouter.PUT("/kv/set/*path", router.HttpKvSet)
	httprouter.DELETE("/kv/del/*path", router.HttpKvDel)
	go func() {
		fmt.Println(http.ListenAndServe(httpBinding, httprouter))
	}()

	router.Run() // does block
}
