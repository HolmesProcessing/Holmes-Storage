package main

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/HolmesProcessing/Holmes-Storage/objStorerGeneric"
	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"

	"github.com/julienschmidt/httprouter"
	"github.com/rakyll/magicmime"
)

type apiResponse struct {
	ResponseCode int
	Failure      string      `json:",omitempty"`
	Result       interface{} `json:",omitempty"`
}

var (
	extendedMime bool
	mimeLock     = &sync.Mutex{}
)

// initHTTP builds and starts the HTTP server that is serving all
// HTTP based requests like storing new samples and downloading files.
func initHTTP(httpBinding string, eMime bool) {
	extendedMime = eMime

	router := httprouter.New()

	router.GET("/samples/:sha256", httpSampleGet)
	router.PUT("/samples/", httpSampleStore)
	router.GET("/config/*path", httpConfigGet)
	router.POST("/config/*path", httpConfigStore)
	router.GET("/maintenance/listObjStorerObjs", httpObjStorerGetObjs)
	router.GET("/maintenance/listOrphans", httpListOrphans)
	router.POST("/maintenance/deleteOrphans", httpDeleteOrphans)

	http.ListenAndServe(httpBinding, router)
}

// httpSampleStore handles "/sample/" requests and is used to validate
// incoming samples and requests. If everything looks good it builds the
// structs and hands them to httpStoreEverything.
func httpSampleStore(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
	mimeType := ""
	if !extendedMime {
		mimeType = http.DetectContentType(fileBytes)
	} else {

		mimeType, err = getMimeFromMagic(fileBytes, 0)
		if err != nil {
			httpFailure(w, r, errors.New("libmagic failed with "+err.Error()))
			return
		}

	}

	// create structs for db
	object := &storerGeneric.Object{
		SHA256:      sha256String,
		SHA1:        sha1String,
		MD5:         md5String,
		MIME:        mimeType,
		Source:      []string{""},
		ObjName:     []string{""},
		Submissions: []string{""},
	}

	date, err := time.Parse(time.RFC3339, r.FormValue("date"))
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	submission := &storerGeneric.Submission{
		SHA256:  sha256String,
		UserId:  userId,
		Source:  r.FormValue("source"),
		Date:    date,
		ObjName: r.FormValue("name"),
		Tags:    r.Form["tags"],
		Comment: r.FormValue("comment"),
	}

	sample := &objStorerGeneric.Sample{
		SHA256: sha256String,
		Data:   fileBytes,
	}

	inserted, err := httpStoreEverything(submission, object, sample)
	if err != nil {
		// Remove all database entries
		mainStorer.DeleteSubmission(submission.Id)
		if inserted {
			// Only delete sample in ObjectStore, if it didn't exist before
			mainStorer.DeleteObject(object.SHA256)
			objStorer.DeleteSample(sample)
		} else {
			// If the sample did exist before, the filename- and source- fields were updated, so that needs to be reverted
			mainStorer.UpdateObject(object.SHA256)
		}

		httpFailure(w, r, err)
		return
	}

	httpSuccess(w, r, object)
}

// httpStoreEverything accepts a submission, object and sample struct pointer and
// tries to save them using the configured storage engines. It returns a boolean value
// indicating if the sample file was already known (resubmitted) and an error.
func httpStoreEverything(submission *storerGeneric.Submission, object *storerGeneric.Object, sample *objStorerGeneric.Sample) (bool, error) {
	// save structs to db
	err := mainStorer.StoreSubmission(submission)
	if err != nil {
		return false, err
	}

	inserted, err := mainStorer.StoreObject(object)
	if err != nil {
		return inserted, err
	}
	// only insert the sample, if it wasn't known before
	if inserted {
		err = objStorer.StoreSample(sample)
	}
	return inserted, err
}

// httpSampleStore handles "/sample/:sha256" requests and returns the matching
// file for download if found.
func httpSampleGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	sample, err := objStorer.GetSample(strings.ToLower(ps.ByName("sha256")))

	if err != nil {
		httpFailure(w, r, err)
		return
	}

	// TODO: Find way to supply a real name with sample
	w.Header().Set("Content-Disposition", "attachment; filename="+sample.SHA256)
	w.Header().Set("Content-Type", "application/octet-stream")
	fmt.Fprint(w, string(sample.Data))
}

func httpConfigStore(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
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

	config := &storerGeneric.Config{
		Path:         path,
		FileContents: string(fileBytes),
	}

	err = mainStorer.StoreConfig(config)
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	httpSuccess(w, r, path)
}

func httpConfigGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	config, err := mainStorer.GetConfig(strings.ToLower(ps.ByName("path")))

	if err != nil {
		httpErrorCode(w, r, err, 404)
		return
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(config.Path))
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, string(config.FileContents))
}

// httpSuccess builds the default http response for a successfull request
// and writes to the ResponseWriter.
func httpSuccess(w http.ResponseWriter, r *http.Request, result interface{}) {
	j, err := json.Marshal(apiResponse{
		ResponseCode: 1,
		Result:       result,
	})

	if err != nil {
		err500(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

// httpObjStorerGetObjs gathers a list of objects from the object-storer
// and writes a json list of sha256-values to the ResponseWriter.
func httpObjStorerGetObjs(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	objs, err := objStorer.GetObjList()
	if err != nil {
		httpFailure(w, r, err)
	}

	objsM, err := json.Marshal(objs)
	if err != nil {
		httpFailure(w, r, err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(objsM)
}

func httpListOrphans(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// TODO
}
func httpDeleteOrphans(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// TODO
}

// httpErrorCode sends an HTTP Error back as a response to the request.
func httpErrorCode(w http.ResponseWriter, r *http.Request, err error, code int) {
	warning.Println("httpFailureEC:", err.Error(), code)

	j, err := json.Marshal(apiResponse{
		ResponseCode: 0,
		Failure:      err.Error(),
	})

	if err != nil {
		err500(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	http.Error(w, string(j), code)
}

// httpFailure builds the default http response for a failed request
// and writes to the ResponseWriter.
func httpFailure(w http.ResponseWriter, r *http.Request, err error) {
	warning.Println("httpFailure:", err.Error())
	w.WriteHeader(http.StatusInternalServerError)
	j, err := json.Marshal(apiResponse{
		ResponseCode: 0,
		Failure:      err.Error(),
	})

	if err != nil {
		err500(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

func err500(w http.ResponseWriter, r *http.Request, err interface{}) {
	warning.Println("500:", err)
	http.Error(w, "Server error occured!", 500)
}

// getMimeFromMagic accepts a sample and counter and then tries to determin the
// mime type of the file. If a panic occures in an external library the function
// will recover and try to get the mime type up to three times before returning
// "N/A" as mime type.
func getMimeFromMagic(fileBytes []byte, try int) (mimeType string, err error) {
	defer func() {
		magicmime.Close()
		mimeLock.Unlock()

		if err := recover(); err != nil {
			warning.Println("magicMime paniced")
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
