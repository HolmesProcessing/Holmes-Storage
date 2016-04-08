package StorerCassandra

import (
	"errors"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/cynexit/Holmes-Storage/storerGeneric"
	//TODO: Take a look at gocassa, gocqltable, cqlc, cqlr
	//      and check if these packages would be a good addition.
)

type StorerCassandra struct {
	DB *gocql.Session
}

func (s StorerCassandra) Initialize(c []*storerGeneric.DBConnector) (storerGeneric.Storer, error) {
	if len(c) < 1 {
		return nil, errors.New("Supply at least one node to connect to!")
	}

	connStrings := make([]string, len(c))
	for i, elem := range c {
		connStrings[i] = fmt.Sprintf("%s:%d", elem.IP, elem.Port)
	}

	if c[0].Database == "" {
		return nil, errors.New("Please supply a database/keyspace to use!")
	}

	var err error
	cluster := gocql.NewCluster(connStrings...)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: c[0].User,
		Password: c[0].Password,
	}
	cluster.ProtoVersion = 4
	cluster.Keyspace = c[0].Database
	cluster.Consistency = gocql.Quorum
	s.DB, err = cluster.CreateSession()

	return s, err
}

func (s StorerCassandra) Setup() error {
	// test if tables already exist
	if err := s.DB.Query("SELECT * FROM results LIMIT 1;").Exec(); err == nil {
		return errors.New("Table results already exists, aborting!")
	}
	if err := s.DB.Query("SELECT * FROM objects LIMIT 1;").Exec(); err == nil {
		return errors.New("Table objects already exists, aborting!")
	}
	if err := s.DB.Query("SELECT * FROM submissions LIMIT 1;").Exec(); err == nil {
		return errors.New("Table submissions already exists, aborting!")
	}

	// create tables
	tableResults := `CREATE TABLE results(
		id uuid PRIMARY KEY,
		sha256 text,
		schema_version text,
		user_id text,
		source_id set<text>,
		source_tag set<text>,
		service_name text,
		service_version text,
		service_config text,
		object_category set<text>,
		object_type text,
		results text,
		tags set<text>,
		started_date_time timestamp,
		finished_date_time timestamp,
		watchguard_status text,
		watchguard_log list<text>,
		watchguard_version text
	);
	`
	if err := s.DB.Query(tableResults).Exec(); err != nil {
		return err
	}

	tableObjects := `CREATE TABLE objects(
		sha256 text PRIMARY KEY,
		sha1 text,
		md5 text,
		mime text,
		source set<text>,
		obj_name set<text>,
		submissions set<uuid>
	);
	`
	if err := s.DB.Query(tableObjects).Exec(); err != nil {
		return err
	}

	tableSubmissions := `CREATE TABLE submissions(
		id uuid PRIMARY KEY,
		sha256 text,
		user_id text,
		source text,
		date timestamp,
		obj_name text,
		tags set<text>,
		comment text
	);
	`
	if err := s.DB.Query(tableSubmissions).Exec(); err != nil {
		return err
	}

	//TODO: create indexes on special fields

	return nil
}

func (s StorerCassandra) StoreObject(object *storerGeneric.Object) error {
	err := s.DB.Query(`INSERT INTO objects (sha256, sha1, md5, mime, source, obj_name, submissions) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		object.SHA256,
		object.SHA1,
		object.MD5,
		object.MIME,
		object.Source,
		object.ObjName,
		object.Submissions,
	).Exec()

	return err
}

func (s StorerCassandra) GetObject(id string) (*storerGeneric.Object, error) {
	object := &storerGeneric.Object{}

	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return object, err
	}

	err = s.DB.Query(`SELECT * FROM objects WHERE id = ? LIMIT 1`, uuid).Scan(
		&object.SHA256,
		&object.SHA1,
		&object.MD5,
		&object.MIME,
		&object.Source,
		&object.ObjName,
		&object.Submissions,
	)

	return object, err
}

func (s StorerCassandra) StoreSubmission(submission *storerGeneric.Submission) error {
	id, err := gocql.RandomUUID()
	if err != nil {
		return err
	}

	err = s.DB.Query(`INSERT INTO submissions (id, sha256, user_id, source, date, obj_name, tags, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		submission.SHA256,
		submission.UserId,
		submission.Source,
		submission.Date,
		submission.ObjName,
		submission.Tags,
		submission.Comment,
	).Exec()

	return err
}

func (s StorerCassandra) GetSubmission(id string) (*storerGeneric.Submission, error) {
	submission := &storerGeneric.Submission{}

	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return submission, err
	}

	err = s.DB.Query(`SELECT * FROM submissions WHERE id = ? LIMIT 1`, uuid).Scan(
		&submission.Id,
		&submission.SHA256,
		&submission.UserId,
		&submission.Source,
		&submission.Date,
		&submission.ObjName,
		&submission.Tags,
		&submission.Comment,
	)

	return submission, err
}

func (s StorerCassandra) StoreResult(result *storerGeneric.Result) error {
	id, err := gocql.RandomUUID()
	if err != nil {
		return err
	}

	err = s.DB.Query(`INSERT INTO results (id, sha256, schema_version, user_id, source_id, source_tag, service_name, service_version, service_config, object_category, object_type, results, tags, started_date_time, finished_date_time, watchguard_status, watchguard_log, watchguard_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		result.SHA256,
		result.SchemaVersion,
		result.UserId,
		result.SourceId,
		result.SourceTag,
		result.ServiceName,
		result.ServiceVersion,
		result.ServiceConfig,
		result.ObjectCategory,
		result.ObjectType,
		result.Results,
		result.Tags,
		result.StartedDateTime,
		result.FinishedDateTime,
		result.WatchguardStatus,
		result.WatchguardLog,
		result.WatchguardVersion,
	).Exec()

	return err
}

func (s StorerCassandra) GetResult(id string) (*storerGeneric.Result, error) {
	result := &storerGeneric.Result{}

	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return result, err
	}

	err = s.DB.Query(`SELECT * FROM results WHERE id = ? LIMIT 1`, uuid).Scan(
		&result.Id,
		&result.SHA256,
		&result.SchemaVersion,
		&result.UserId,
		&result.SourceId,
		&result.SourceTag,
		&result.ServiceName,
		&result.ServiceVersion,
		&result.ServiceConfig,
		&result.ObjectCategory,
		&result.ObjectType,
		&result.Results,
		&result.Tags,
		&result.StartedDateTime,
		&result.FinishedDateTime,
		&result.WatchguardStatus,
		&result.WatchguardLog,
		&result.WatchguardVersion,
	)

	return result, err
}
