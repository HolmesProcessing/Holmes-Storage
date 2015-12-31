package main

import (
	"github.com/julienschmidt/httprouter"
)

type apiResponse struct {
	ResponseCode int
	Failure      string      `json:",omitempty"`
	Result       interface{} `json:",omitempty"`
}

func initHTTP(httpBinding string) {
	router := httprouter.New()

	router.GET("/samples/:sha256", httpSampleGet)
	router.PUT("/samples/", httpSampleStore)

	log.Fatal(http.ListenAndServe(httpBinding, router))
}

func httpSampleStore(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	r.ParseForm()

	// TODO: fix shit
	fmt.Println("username:", r.Form["username"])
	fmt.Println("password:", r.Form["password"])
	sample, err := myStorer.StoreSample()

	if err != nil {
		httpFailure(w, req, err)
		return
	}

	httpSuccess(w, req, sample)
}

func httpSampleGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sample, err := myStorer.GetSample(ps.ByName("sha256"))

	if err != nil {
		httpFailure(w, req, err)
		return
	}

	//httpSuccess(w, req, sample)
	// TODO: Find way to supply a real name with sample
	w.Header().Set("Content-Disposition", "attachment; filename="+sample.SHA256)
	fmt.Fprint(w, sample.Data)
}

func httpSuccess(w http.ResponseWriter, r *http.Request, result interface{}) {
	j, err := json.Marshal(apiResponse{
		ResponseCode: 1,
		Result:       result,
	})

	if err != nil {
		err500(w, req, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

func httpFailure(w http.ResponseWriter, r *http.Request, err error) {
	j, err := json.Marshal(apiResponse{
		ResponseCode: 0,
		Failure:      err.Error(),
	})

	if err != nil {
		err500(w, req, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}
