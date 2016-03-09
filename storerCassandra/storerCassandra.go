package storerCassandra

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gocql/gocql"

	"github.com/cynexit/Holmes-Storage/storerGeneric"
)

type storerCassandra struct {
	DB *gocql.Session
}

// wrapper of db* to use native bson _id
type dbSubmissionsCassandra struct {
	Id     bson.ObjectId `json:"_id" bson:"_id,omitempty"`
	SHA256 string        `json:"sha256"`
	UserId int           `json:"user_id"`
	Source string        `json:"source"`
	Name   string        `json:"name"`
	Date   string        `json:"date"`
}

type dbSamplesCassandra struct {
	SHA256 string      `json:"sha256"`
	Data   bson.Binary `json:"data"`
}

type dbResultsCassandra struct {
	Id                bson.ObjectId          `json:"_id" bson:"_id,omitempty"`
	SHA256            string                 `json:"sha256"`
	SchemaVersion     string                 `json:"schema_version"`
	UserId            int                    `json:"user_id"`
	SourceId          int                    `json:"source_id"`
	ServiceName       string                 `json:"service_name"`
	ServiceVersion    string                 `json:"service_version"`
	ServiceConfig     string                 `json:"service_config"`
	ObjectCategory    string                 `json:"object_category"`
	ObjectType        string                 `json:"object_type"`
	Results           map[string]interface{} `json:"results"`
	Tags              []string               `json:"tags"`
	Date              string                 `json:"date"`
	WatchguardStatus  string                 `json:"watchguard_status"`
	WatchguardLog     []string               `json:"watchguard_log"`
	WatchguardVersion string                 `json:"watchguard_version"`
}

func (s storerCassandra) Initialize(c []*dbConnector) (Storer, error) {
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

	if c[0].Database == "" {
		return nil, errors.New("Please supply a database/keyspace to use!")
	}

	cluster := gocql.NewCluster(connStrings...)
	cluster.Keyspace = c[0].Database
	cluster.Consistency = gocql.Quorum
	s.DB, err = cluster.CreateSession()
	if err != nil {
		return s, err
	}

	return s, nil
}

func (s storerCassandra) Setup() error {
	// TODO: Create collections, set indexes
	// db.runCommand( { enablesharding : "holmes" } );
	// db.runCommand( { shardcollection : "holmes.results", key : { "object_id" : 1 } } );

	shaIndex := mgo.Index{
		Key:        []string{"SHA256"},
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

func (s storerCassandra) StoreObject(object *dbObjects) error {
	if err := s.DB.C("objects").Insert(object); err != nil {
		return err
	}

	return nil
}

func (s storerCassandra) GetObject(id string) (*dbObjects, error) {
	var object dbObjects
	s.DB.C("objects").Find(bson.M{"sha256": id}).One(&object)

	if object.SHA256 == "" {
		return nil, errors.New("Not found")
	}

	return &object, nil
}

func (s storerCassandra) StoreSubmission(submission *dbSubmissions) error {
	submissionM := &dbSubmissionsCassandra{
		Id:     bson.NewObjectId(),
		SHA256: submission.SHA256,
		UserId: submission.UserId,
		Source: submission.Source,
		Name:   submission.Name,
		Date:   submission.Date,
	}

	if err := s.DB.C("submissions").Insert(submissionM); err != nil {
		return err
	}

	return nil
}

func (s storerCassandra) GetSubmission(id string) (*dbSubmissions, error) {
	var submission dbSubmissionsCassandra

	s.DB.C("submissions").Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&submission)

	if submission.Id == "" {
		return nil, errors.New("ID not found!")
	}

	return &dbSubmissions{
		Id:     submission.Id.Hex(),
		SHA256: submission.SHA256,
		UserId: submission.UserId,
		Source: submission.Source,
		Name:   submission.Name,
		Date:   submission.Date,
	}, nil
}

func (s storerCassandra) StoreSample(sample *dbSamples) error {
	sampleM := &dbSamplesCassandra{
		SHA256: sample.SHA256,
		Data:   bson.Binary{Kind: 0, Data: sample.Data},
	}

	if err := s.DB.C("samples").Insert(sampleM); err != nil {
		return err
	}

	return nil
}

func (s storerCassandra) GetSample(id string) (*dbSamples, error) {
	var sampleM dbSamplesCassandra

	s.DB.C("samples").Find(bson.M{"sha256": id}).One(&sampleM)
	if sampleM.SHA256 == "" {
		return nil, errors.New("Not found")
	}

	return &dbSamples{
		SHA256: sampleM.SHA256,
		Data:   sampleM.Data.Data,
	}, nil
}

func (s storerCassandra) StoreResult(result *dbResults) error {
	resultsM := &dbResultsCassandra{
		Id:                bson.NewObjectId(),
		SHA256:            result.SHA256,
		SchemaVersion:     result.SchemaVersion,
		UserId:            result.UserId,
		SourceId:          result.SourceId,
		ServiceName:       result.ServiceName,
		ServiceVersion:    result.ServiceVersion,
		ServiceConfig:     result.ServiceConfig,
		ObjectCategory:    result.ObjectCategory,
		ObjectType:        result.ObjectType,
		Results:           result.Results,
		Tags:              result.Tags,
		Date:              result.Date,
		WatchguardStatus:  result.WatchguardStatus,
		WatchguardLog:     result.WatchguardLog,
		WatchguardVersion: result.WatchguardVersion,
	}

	if err := s.DB.C("results").Insert(resultsM); err != nil {
		return err
	}

	return nil
}

func (s storerCassandra) GetResult(id string) (*dbResults, error) {
	var result dbResultsCassandra

	s.DB.C("results").Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&result)

	if result.Id == "" {
		return nil, errors.New("ID not found!")
	}

	return &dbResults{
		Id:                result.Id.Hex(),
		SHA256:            result.SHA256,
		SchemaVersion:     result.SchemaVersion,
		UserId:            result.UserId,
		SourceId:          result.SourceId,
		ServiceName:       result.ServiceName,
		ServiceVersion:    result.ServiceVersion,
		ServiceConfig:     result.ServiceConfig,
		ObjectCategory:    result.ObjectCategory,
		ObjectType:        result.ObjectType,
		Results:           result.Results,
		Tags:              result.Tags,
		Date:              result.Date,
		WatchguardStatus:  result.WatchguardStatus,
		WatchguardLog:     result.WatchguardLog,
		WatchguardVersion: result.WatchguardVersion,
	}, nil
}
