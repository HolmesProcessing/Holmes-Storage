package http

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/HolmesProcessing/Holmes-Storage/context"
	"github.com/HolmesProcessing/Holmes-Storage/dataStorage"
	"github.com/HolmesProcessing/Holmes-Storage/objectStorage"

	"github.com/julienschmidt/httprouter"
	"github.com/rakyll/magicmime"
)

type apiResponse struct {
	ResponseCode int
	Failure      string      `json:",omitempty"`
	Result       interface{} `json:",omitempty"`
}

var (
	ctx      *context.Ctx
	mimeLock = &sync.Mutex{}
)

func Start(c *context.Ctx) {
	ctx = c

	//router
	router := httprouter.New()

	//... for data
	router.GET("/api/v2/objects", objectGet) //get a list of recent objects or search
	router.GET("/api/v2/objects/:sha256", objectGet) //get a specific object
	router.POST("/api/v2/objects/", dummyHandler) //create a new object
	router.PUT("/api/v2/objects", dummyHandler) //return 405 error
	router.PUT("/api/v2/objects/:sha256", dummyHandler) //updates specific object
	router.DELETE("/api/v2/objects/:sha256", dummyHandler) //delete specific object

	router.GET("/api/v2/results", dummyHandler) //get a list of recent results or search
	router.GET("/api/v2/results/:uuid", dummyHandler) //get a specific result
	router.POST("/api/v2/results/", dummyHandler) //create a new result
	router.PUT("/api/v2/results", dummyHandler) //return 405 error
	router.PUT("/api/v2/results/:uuid", dummyHandler) //updates specific result
	router.DELETE("/api/v2/results/:uuid", dummyHandler) //delete a specific result

	router.GET("/api/v2/submissions", dummyHandler) //get a list of recent submissions or search
	router.GET("/api/v2/submissions/:uuid", submissionGet) //get a specific submissions
	router.POST("/api/v2/submissions/", dummyHandler) //create a new submissions
	router.PUT("/api/v2/submissions", dummyHandler) //return 405 error
	router.PUT("/api/v2/submissions/:uuid", dummyHandler) //updates specific submissions
	router.DELETE("/api/v2/submissions/:uuid", dummyHandler) //delete a specific submissions

	//we don't have configs implemented yet. So I am just going to leave this here
	//for future reference. 
	router.GET("/api/v2/configs", dummyHandler) //get config
	router.POST("/api/v2/configs/", dummyHandler) //create config
	router.PUT("/api/v2/configs/", dummyHandler) //update config
	router.DELETE("/api/v2/configs/", dummyHandler) //delete config


	//... for raw_data
	router.GET("/api/v2/raw_data", sampleGet) //return 405 error
	router.GET("/api/v2/raw_data/:sha256", sampleGet) //get a specific raw data
	router.POST("/api/v2/raw_data/", sampleStore) //create a new raw_data entry
	router.PUT("/api/v2/raw_data", dummyHandler) //return 405 error
	router.DELETE("/api/v2/raw_data/:sha256", dummyHandler)

	// configure the http server
	if c.Config.SSLCert != "" && c.Config.SSLKey != "" {

		cfg := &tls.Config{
			MinVersion:               tls.VersionTLS12,
			CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
			PreferServerCipherSuites: true,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
		}

		srv := &http.Server{
			Addr:         c.Config.HTTP,
			Handler:      router,
			TLSConfig:    cfg,
			TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
		}

		srv.ListenAndServeTLS(c.Config.SSLCert, c.Config.SSLKey)

	} else {

		ctx.Warning.Println("Starting HTTP server without SSL")
		http.ListenAndServe(c.Config.HTTP, router)

	}

	/*
		w.Header().Add("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "origin, content-type, accept")
		w.Header().Set("Content-Type", "application/json")

		if r.Method == "OPTIONS" {
			return
		}
	*/
}

func dummyHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	httpFailure(w, r, errors.New("Method not implemented"))
}

func objectGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	obj, err := ctx.Data.ObjectGet(strings.ToLower(ps.ByName("sha256")))

	if err != nil {
		httpFailure(w, r, err)
		return
	}

	httpSuccess(w, r, obj)
}

func submissionGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	submission, err := ctx.Data.SubmissionGet(strings.ToLower(ps.ByName("uuid")))

	if err != nil {
		httpFailure(w, r, err)
		return
	}

	httpSuccess(w, r, submission)
}

func sampleGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sample, err := ctx.Objects.SampleGet(strings.ToLower(ps.ByName("sha256")))

	if err != nil {
		httpFailure(w, r, err)
		return
	}

	// TODO: Find way to supply a real name with sample
	w.Header().Set("Content-Disposition", "attachment; filename="+sample.SHA256)
	w.Header().Set("Content-Type", "application/octet-stream")
	fmt.Fprint(w, string(sample.Data))
}

// sampleStore is used to validate and store incoming samples. If everything
// looks good it builds the structs and hands them to sampleStoreEverything.
func sampleStore(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// keep samples up to 20mb in RAM to speed up processing
	// if you see RAM exhaustion on your host lower this value
	// if you see slow processing of larger samples up this value
	r.ParseMultipartForm(1024 * 1024 * 20)

	// validate inputs
	userId := r.FormValue("user_id")
	if userId == "" ||
		r.FormValue("source") == "" ||
		r.FormValue("name") == "" ||
		r.FormValue("date") == "" {

		errMsg := fmt.Sprintf("user_id: %d, source: %s, name: %s, date: %s", userId, r.FormValue("source"), r.FormValue("name"), r.FormValue("date"))
		httpFailure(w, r, errors.New("Please supply all necessary values! "+errMsg))
		return
	}

	file, _, err := r.FormFile("sample")
	if err != nil {
		httpFailure(w, r, err)
		return
	}
	defer file.Close()

	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	if len(fileBytes) == 0 {
		httpFailure(w, r, errors.New("empty file"))
		return
	}

	// generate the necessary hashes
	hSHA256 := sha256.New()
	hSHA256.Write(fileBytes)
	sha256String := fmt.Sprintf("%x", hSHA256.Sum(nil))

	hSHA1 := sha1.New()
	hSHA1.Write(fileBytes)
	sha1String := fmt.Sprintf("%x", hSHA1.Sum(nil))

	hMD5 := md5.New()
	hMD5.Write(fileBytes)
	md5String := fmt.Sprintf("%x", hMD5.Sum(nil))

	// get mimetype
	mimeType, err := getMimeFromMagic(fileBytes, 0)
	if err != nil {
		httpFailure(w, r, errors.New("libmagic failed with "+err.Error()))
		return
	}

	// create structs for db
	object := &dataStorage.Object{
		Type:             "file",
		CreationDateTime: time.Now(),
		SHA256:           sha256String,
		SHA1:             sha1String,
		MD5:              md5String,
		FileMime:         mimeType,
		Source:           []string{""},
		FileName:         []string{""},
		Submissions:      []string{""},
	}

	date, err := time.Parse(time.RFC3339, r.FormValue("date"))
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	submission := &dataStorage.Submission{
		SHA256:   sha256String,
		UserId:   userId,
		Source:   r.FormValue("source"),
		DateTime: date,
		ObjName:  r.FormValue("name"),
		Tags:     r.Form["tags"],
		Comment:  r.FormValue("comment"),
	}

	sample := &objectStorage.Sample{
		SHA256: sha256String,
		Data:   fileBytes,
	}

	inserted, err := httpStoreEverything(submission, object, sample)
	if err != nil {
		// Remove all database entries
		ctx.Data.SubmissionDelete(submission.Id)
		if inserted {
			// Only delete sample in ObjectStore, if it didn't exist before
			ctx.Data.ObjectDelete(object.SHA256)
			ctx.Objects.SampleDelete(sample)
		} else {
			// If the sample did exist before, the filename- and source- fields were updated, so that needs to be reverted
			ctx.Data.ObjectUpdate(object.SHA256)
		}

		httpFailure(w, r, err)
		return
	}

	httpSuccess(w, r, object)
}

// httpStoreEverything accepts a submission, object and sample struct pointer and
// tries to save them using the configured storage engines. It returns a boolean value
// indicating if the sample file was already known (resubmitted) and an error.
func httpStoreEverything(submission *dataStorage.Submission, object *dataStorage.Object, sample *objectStorage.Sample) (bool, error) {
	// save structs to db
	err := ctx.Data.SubmissionStore(submission)
	if err != nil {
		return false, err
	}

	inserted, err := ctx.Data.ObjectStore(object)
	if err != nil {
		return inserted, err
	}

	// only insert the sample, if it wasn't known before
	if inserted {
		err = ctx.Objects.SampleStore(sample)
	}

	return inserted, err
}

func configGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	config, err := ctx.Data.ConfigGet(strings.ToLower(ps.ByName("path")))

	if err != nil {
		httpFailure(w, r, err)
		return
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(config.Path))
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, string(config.FileContents))
}

func configStore(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	path := strings.ToLower(ps.ByName("path"))
	file, _, err := r.FormFile("config")
	if err != nil {
		httpFailure(w, r, err)
		return
	}
	defer file.Close()

	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	config := &dataStorage.Config{
		Path:         path,
		FileContents: string(fileBytes),
	}

	err = ctx.Data.ConfigStore(config)
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	httpSuccess(w, r, path)
}

/*
func httpGenericRequestHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var cReq context.Request
	err := decoder.Decode(&cReq)
	if err != nil {
		httpFailureHard(w, r, err)
		return
	}

	ctx.Debug.Printf("Request: %+v\n", cReq)

	j, err := json.Marshal(router.Route(ctx, &cReq))
	if err != nil {
		httpFailureHard(w, r, err)
		return
	}

	w.Write(j)
}
*/

// httpSuccess builds the default http response for a successfull request
// and writes to the ResponseWriter.
func httpSuccess(w http.ResponseWriter, r *http.Request, result interface{}) {
	j, err := json.Marshal(apiResponse{
		ResponseCode: 0,
		Result:       result,
	})

	if err != nil {
		httpFailureHard(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

// httpFailure builds the default http response for a failed request
// and writes to the ResponseWriter.
func httpFailure(w http.ResponseWriter, r *http.Request, err error) {
	ctx.Debug.Println("httpFailure:", err.Error())

	// TODO: Decide the http return code question....
	//w.WriteHeader(http.StatusInternalServerError)
	j, err := json.Marshal(apiResponse{
		ResponseCode: 1,
		Failure:      err.Error(),
	})

	if err != nil {
		httpFailureHard(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

// httpFailureHard is supposed to be called on hard errors like a broken server,
// not on "soft failures" like an non-existing sample sha.
func httpFailureHard(w http.ResponseWriter, r *http.Request, err interface{}) {
	ctx.Warning.Println("500:", err)
	http.Error(w, "Server error!", 500)
}

// getMimeFromMagic accepts a sample and counter and then tries to determin the
// mime type of the file. If a panic occures in an external library the function
// will recover and try to get the mime type up to three times before returning
// "N/A" as mime type.
func getMimeFromMagic(fileBytes []byte, try int) (mimeType string, err error) {
	defer func() {
		magicmime.Close()
		mimeLock.Unlock()

		if r := recover(); r != nil {
			ctx.Warning.Println("magicMime paniced")
			time.Sleep(time.Second)
			mimeType, err = getMimeFromMagic(fileBytes, try+1)
		}
	}()

	// if we tried to get the mimeType 3 times but paniced we'll return a
	// static string
	if try >= 3 {
		err = nil
		mimeType = "N/A"
		return
	}

	mimeLock.Lock()

	err = magicmime.Open(magicmime.MAGIC_ERROR)

	if err != nil {
		err = errors.New("ExtendedMime is activated but libmagic is not installed!")
		return
	}

	mimeType, err = magicmime.TypeByBuffer(fileBytes)
	return
}
