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

// wrapper of dbResults to use native bson _id
type dbResultsMongodb struct {
	Id                bson.ObjectId `json:"_id" bson:"_id,omitempty"`
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
	return nil
}

func (s storerMongoDB) StoreResult(result *dbResults) error {
	resultsM := &dbResultsMongodb{
		Id:                bson.NewObjectId(),
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
