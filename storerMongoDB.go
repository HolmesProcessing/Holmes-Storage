package main

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type storerMongoDB struct {
	DB *mgo.Database
}

// wrapper of db* to use native bson _id
type dbSubmissionsMongodb struct {
	Id     bson.ObjectId `json:"_id" bson:"_id,omitempty"`
	SHA256 string        `json:"sha256"`
	UserId int           `json:"user_id"`
	Source string        `json:"source"`
	Name   string        `json:"name"`
	Date   string        `json:"date"`
}

type dbSamplesMongodb struct {
	SHA256 string      `json:"sha256"`
	Data   bson.Binary `json:"data"`
}

type dbResultsMongodb struct {
	Id                bson.ObjectId `json:"_id" bson:"_id,omitempty"`
	SHA256            string        `json:"sha256"`
	SchemaVersion     string        `json:"schema_version"`
	UserId            int           `json:"user_id"`
	SourceId          int           `json:"source_id"`
	ServiceName       string        `json:"service_name"`
	ServiceVersion    string        `json:"service_version"`
	ServiceConfig     string        `json:"service_config"`
	ObjectCategory    string        `json:"object_category"`
	ObjectType        string        `json:"object_type"`
	Results           string        `json:"results"`
	Date              string        `json:"date"`
	WatchguardStatus  string        `json:"watchguard_status"`
	WatchguardLog     []string      `json:"watchguard_log"`
	WatchguardVersion string        `json:"watchguard_version"`
}

func (s storerMongoDB) Initialize(c []*dbConnector) (Storer, error) {
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

func (s storerMongoDB) Setup() error {
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

func (s storerMongoDB) StoreObject(object *dbObjects) error {
	if err := s.DB.C("objects").Insert(object); err != nil {
		return err
	}

	return nil
}

func (s storerMongoDB) GetObject(id string) (*dbObjects, error) {
	var object dbObjects
	s.DB.C("objects").Find(bson.M{"sha256": id}).One(&object)

	if object.SHA256 == "" {
		return nil, errors.New("Not found")
	}

	return &object, nil
}

func (s storerMongoDB) StoreSubmission(submission *dbSubmissions) error {
	submissionM := &dbSubmissionsMongodb{
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

func (s storerMongoDB) GetSubmission(id string) (*dbSubmissions, error) {
	var submission dbSubmissionsMongodb

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

func (s storerMongoDB) StoreSample(sample *dbSamples) error {
	sampleM := &dbSamplesMongodb{
		SHA256: sample.SHA256,
		Data:   bson.Binary{Kind: 0, Data: sample.Data},
	}

	if err := s.DB.C("samples").Insert(sampleM); err != nil {
		return err
	}

	return nil
}

func (s storerMongoDB) GetSample(id string) (*dbSamples, error) {
	var sampleM dbSamplesMongodb

	s.DB.C("samples").Find(bson.M{"sha256": id}).One(&sampleM)
	if sampleM.SHA256 == "" {
		return nil, errors.New("Not found")
	}

	return &dbSamples{
		SHA256: sampleM.SHA256,
		Data:   sampleM.Data.Data,
	}, nil
}

func (s storerMongoDB) StoreResult(result *dbResults) error {
	resultsM := &dbResultsMongodb{
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

func (s storerMongoDB) GetResult(id string) (*dbResults, error) {
	var result dbResultsMongodb

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
		Date:              result.Date,
		WatchguardStatus:  result.WatchguardStatus,
		WatchguardLog:     result.WatchguardLog,
		WatchguardVersion: result.WatchguardVersion,
	}, nil
}
