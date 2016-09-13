package storerMongoDB

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"
)

type StorerMongoDB struct {
	DB *mgo.Database
}

// wrapper for generic collections to use native bson _id
type Submission struct {
	Id      bson.ObjectId `json:"_id" bson:"_id,omitempty"`
	SHA256  string        `json:"sha256"`
	UserId  string        `json:"user_id"`
	Source  string        `json:"source"`
	Date    string        `json:"date"`
	ObjName string        `json:"obj_name"`
	Tags    []string      `json:"tags"`
	Comment string        `json:"comment"`
}

type Result struct {
	Id                bson.ObjectId `json:"_id" bson:"_id,omitempty"`
	SHA256            string        `json:"sha256"`
	SchemaVersion     string        `json:"schema_version"`
	UserId            string        `json:"user_id"`
	SourceId          []string      `json:"source_id"`
	SourceTag         []string      `json:"source_tag"`
	ServiceName       string        `json:"service_name"`
	ServiceVersion    string        `json:"service_version"`
	ServiceConfig     string        `json:"service_config"`
	ObjectCategory    []string      `json:"object_category"`
	ObjectType        string        `json:"object_type"`
	Results           interface{}   `json:"results"`
	Tags              []string      `json:"tags"`
	StartedDateTime   string        `json:"started_date_time"`
	FinishedDateTime  string        `json:"finished_date_time"`
	WatchguardStatus  string        `json:"watchguard_status"`
	WatchguardLog     []string      `json:"watchguard_log"`
	WatchguardVersion string        `json:"watchguard_version"`
}

func (s StorerMongoDB) CreateDB(c []*storerGeneric.DBConnector) error {
	// Nothing to do here, since mongo creates the database on the fly
	return nil
}

func (s StorerMongoDB) Initialize(c []*storerGeneric.DBConnector) (storerGeneric.Storer, error) {
	if len(c) < 1 {
		return nil, errors.New("Supply at least one node to connect to!")
	}

	connStrings := make([]string, len(c))
	for i, elem := range c {
		if elem.User != "" {
			connStrings[i] = fmt.Sprintf("%s:%s@%s:%d", elem.User, elem.Password, elem.IP, elem.Port)
			continue
		}

		// no auth data given, do anonymous login
		connStrings[i] = fmt.Sprintf("%s:%d", elem.IP, elem.Port)
	}

	session, err := mgo.Dial(strings.Join(connStrings, ","))
	if err != nil {
		return s, err
	}

	session.SetMode(mgo.Monotonic, true)

	if c[0].Database == "" {
		return nil, errors.New("Please supply a database to use!")
	}
	s.DB = session.DB(c[0].Database)

	return s, nil
}

func (s StorerMongoDB) Setup() error {
	// TODO: Create collections, set indexes
	// db.runCommand( { enablesharding : "holmes" } );
	// db.runCommand( { shardcollection : "holmes.results", key : { "object_id" : 1 } } );

	shaIndex := mgo.Index{
		Key:        []string{"sha256"},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     false,
	}

	if err := s.DB.C("objects").EnsureIndex(shaIndex); err != nil {
		return err
	}

	if err := s.DB.C("samples").EnsureIndex(shaIndex); err != nil {
		return err
	}

	idIndex := mgo.Index{
		Key:        []string{"_id"},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     false,
	}

	if err := s.DB.C("submissions").EnsureIndex(idIndex); err != nil {
		return err
	}

	if err := s.DB.C("results").EnsureIndex(idIndex); err != nil {
		return err
	}

	return nil
}

func (s StorerMongoDB) StoreObject(object *storerGeneric.Object) error {
	// gather submissions and update source, objname, submissions
	submissions := s.GetSubmissionsByObject(object.SHA256)
	l := len(submissions)
	object.Source = make([]string, l)
	object.ObjName = make([]string, l)
	object.Submissions = make([]string, l)
	
	for k, v := range submissions {
		object.Source[k] = v.Source
		object.ObjName[k] = v.ObjName
		object.Submissions[k] = v.Id.String()
	}

	_, err := s.DB.C("objects").Upsert(bson.M{"sha256":object.SHA256}, object)
	return err
}

func (s StorerMongoDB) GetObject(id string) (*storerGeneric.Object, error) {
	var object storerGeneric.Object
	s.DB.C("objects").Find(bson.M{"sha256": id}).One(&object)

	if object.SHA256 == "" {
		return nil, errors.New("Not found")
	}

	return &object, nil
}

func (s StorerMongoDB) StoreSubmission(submission *storerGeneric.Submission) error {
	fmt.Println()

	submissionM := &Submission{
		Id:      bson.NewObjectId(),
		SHA256:  submission.SHA256,
		UserId:  submission.UserId,
		Source:  submission.Source,
		Date:    submission.Date.Format(time.RFC3339),
		ObjName: submission.ObjName,
		Tags:    submission.Tags,
		Comment: submission.Comment,
	}

	if err := s.DB.C("submissions").Insert(submissionM); err != nil {
		return err
	}

	return nil
}

func (s StorerMongoDB) GetSubmission(id string) (*storerGeneric.Submission, error) {
	var submission Submission

	s.DB.C("submissions").Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&submission)

	if submission.Id == "" {
		return nil, errors.New("ID not found!")
	}

	t, _ := time.Parse(time.RFC3339, submission.Date)

	return &storerGeneric.Submission{
		Id:      submission.Id.Hex(),
		SHA256:  submission.SHA256,
		UserId:  submission.UserId,
		Source:  submission.Source,
		Date:    t,
		ObjName: submission.ObjName,
		Tags:    submission.Tags,
		Comment: submission.Comment,
	}, nil
}

func (s StorerMongoDB) GetSubmissionsByObject(sha256 string) ([]*Submission) {
	submissions := []*Submission{}
	s.DB.C("submissions").Find(bson.M{"sha256": sha256}).All(&submissions)
	return submissions

}

func (s StorerMongoDB) StoreResult(result *storerGeneric.Result) error {
	var resultJSON interface{}
	err := json.Unmarshal([]byte(result.Results), &resultJSON)
	if err != nil {
		return err
	}

	resultsM := &Result{
		Id:                bson.NewObjectId(),
		SHA256:            result.SHA256,
		SchemaVersion:     result.SchemaVersion,
		UserId:            result.UserId,
		SourceId:          result.SourceId,
		SourceTag:         result.SourceTag,
		ServiceName:       result.ServiceName,
		ServiceVersion:    result.ServiceVersion,
		ServiceConfig:     result.ServiceConfig,
		ObjectCategory:    result.ObjectCategory,
		ObjectType:        result.ObjectType,
		Results:           resultJSON,
		Tags:              result.Tags,
		StartedDateTime:   result.StartedDateTime.Format(time.RFC3339),
		FinishedDateTime:  result.FinishedDateTime.Format(time.RFC3339),
		WatchguardStatus:  result.WatchguardStatus,
		WatchguardLog:     result.WatchguardLog,
		WatchguardVersion: result.WatchguardVersion,
	}

	return s.DB.C("results").Insert(resultsM)
}

func (s StorerMongoDB) GetResult(id string) (*storerGeneric.Result, error) {
	var result Result

	s.DB.C("results").Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&result)

	if result.Id == "" {
		return nil, errors.New("ID not found!")
	}

	resultsBytes, err := json.Marshal(result.Results)
	if err != nil {
		return nil, err
	}

	StartedDateTimeParsed, _ := time.Parse(time.RFC3339, result.StartedDateTime)
	FinishedDateTimeParsed, _ := time.Parse(time.RFC3339, result.FinishedDateTime)

	return &storerGeneric.Result{
		Id:                result.Id.Hex(),
		SHA256:            result.SHA256,
		SchemaVersion:     result.SchemaVersion,
		UserId:            result.UserId,
		SourceId:          result.SourceId,
		SourceTag:         result.SourceTag,
		ServiceName:       result.ServiceName,
		ServiceVersion:    result.ServiceVersion,
		ServiceConfig:     result.ServiceConfig,
		ObjectCategory:    result.ObjectCategory,
		ObjectType:        result.ObjectType,
		Results:           string(resultsBytes),
		Tags:              result.Tags,
		StartedDateTime:   StartedDateTimeParsed,
		FinishedDateTime:  FinishedDateTimeParsed,
		WatchguardStatus:  result.WatchguardStatus,
		WatchguardLog:     result.WatchguardLog,
		WatchguardVersion: result.WatchguardVersion,
	}, nil
}

func (s StorerMongoDB) StoreConfig(config *storerGeneric.Config) error {
	err := s.DB.C("config").Insert(config)
	return err
}

func (s StorerMongoDB) GetConfig(path string) (*storerGeneric.Config, error) {
	var config storerGeneric.Config
	s.DB.C("config").Find(bson.M{"path": path}).One(&config)

	if config.Path == "" {
		return nil, errors.New("Not found")
	}

	return &config, nil
}
