package ObjStorerS3

import (
	"errors"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"

	"github.com/cynexit/Holmes-Storage/objStorerGeneric"
	"github.com/cynexit/Holmes-Storage/storerGeneric"
)

type ObjStorerS3 struct {
	DB *s3.S3
}

func (s ObjStorerS3) Initialize(c []*storerGeneric.DBConnector) (objStorerGeneric.ObjStorer, error) {
	if len(c) < 1 {
		return nil, errors.New("Supply at least one node to connect to!")
	}

	auth, err := aws.GetAuth(c[0].User, c[0].Password)
	if err != nil {
		return nil, errors.New("Please supply a database/keyspace to use!")
	}

	cluster := gocql.NewCluster(connStrings...)
	cluster.ProtoVersion = 4
	cluster.Keyspace = c[0].Database
	cluster.Consistency = gocql.Quorum
	s.DB, err = cluster.CreateSession()

	return s, err
}

func (s ObjStorerS3) Setup() error {
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

func (s ObjStorerS3) StoreSample(*objStorerGeneric.Sample) error {
	return nil
}

func (s ObjStorerS3) GetSample(string) (*objStorerGeneric.Sample, error) {
	return nil, nil
}
