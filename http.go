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
	"strings"
	"sync"
	"time"
	"path/filepath"

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

func initHTTP(httpBinding string, eMime bool) {
	extendedMime = eMime

	router := httprouter.New()

	router.GET("/samples/:sha256", httpSampleGet)
	router.PUT("/samples/", httpSampleStore)
	router.GET("/config/*path", httpConfigGet)
	router.POST("/config/*path", httpConfigStore)

	http.ListenAndServe(httpBinding, router)
}

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

		mimeLock.Lock()

		if err = magicmime.Open(magicmime.MAGIC_ERROR); err != nil {
			httpFailure(w, r, errors.New("ExtendedMime is activated but libmagic is not installed!"))
			mimeLock.Unlock()
			return
		}

		mimeType, err = magicmime.TypeByBuffer(fileBytes)
		if err != nil {
			magicmime.Close()
			httpFailure(w, r, errors.New("libmagic failed with "+err.Error()))
			mimeLock.Unlock()
			return
		}

		magicmime.Close()
		mimeLock.Unlock()
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
		Tags:    r.URL.Query()["tags[]"],
		Comment: r.FormValue("comment"),
	}

	sample := &objStorerGeneric.Sample{
		SHA256: sha256String,
		Data:   fileBytes,
	}

	// save structs to db
	// submission needs to be saved first!
	err = mainStorer.StoreSubmission(submission)
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	err = mainStorer.StoreObject(object)
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	err = objStorer.StoreSample(sample)
	if err != nil {
		httpFailure(w, r, err)
		return
	}

	httpSuccess(w, r, object)
}

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
		Path: path,
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

func httpErrorCode(w http.ResponseWriter, r *http.Request, err error, code int) {
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

func httpFailure(w http.ResponseWriter, r *http.Request, err error) {
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
	warning.Println(err)
	http.Error(w, "Server error occured!", 500)
}
