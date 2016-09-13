package StorerCassandra

import (
	"errors"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/HolmesProcessing/Holmes-Storage/storerGeneric"
	//TODO: Take a look at gocassa, gocqltable, cqlc, cqlr
	//      and check if these packages would be a good addition.
)

type StorerCassandra struct {
	DB *gocql.Session
}

func (s StorerCassandra) PrepareCluster(c []*storerGeneric.DBConnector) (*gocql.ClusterConfig, error) {
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
	return cluster, err
}

func (s StorerCassandra) CreateDB(c []*storerGeneric.DBConnector) error {
	cluster, err := s.PrepareCluster(c)
	if err != nil {
		return err
	}
	cluster.Keyspace = "system"
	s.DB, err = cluster.CreateSession()
	if err != nil{
		return err
	}
	query := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = 
		{'class': 'SimpleStrategy',
		'replication_factor':1};`, c[0].Database)
	if err := s.DB.Query(query).Exec(); err != nil {
		return err
	}
	s.DB.Close()
	return nil
}

func (s StorerCassandra) Initialize(c []*storerGeneric.DBConnector) (storerGeneric.Storer, error) {
	cluster, err := s.PrepareCluster(c)
	if err != nil {
		return s, err
	}
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
	if err := s.DB.Query("SELECT * FROM config LIMIT 1;").Exec(); err == nil {
		return errors.New("Table config already exists, aborting!")
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
		watchguard_version text,
		comment text
	);`
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
	);`
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
	);`
	if err := s.DB.Query(tableSubmissions).Exec(); err != nil {
		return err
	}

	tableConfig := `CREATE TABLE config(
		path text PRIMARY KEY,
		file_contents text
	);`
	if err := s.DB.Query(tableConfig).Exec(); err != nil {
		return err
	}

	//TODO: add complex SASI indexes on tags, object_category, etc when supported by Cassandra
	//TODO: add indexes for other entries (watchguard_status, user_id, service_version) under results when totem catches up

	// Add SASI indexes for results
	tableResultsIndex := `CREATE CUSTOM INDEX results_comment_idx 
		ON results (comment) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex' 
		WITH OPTIONS = {
			'analyzed' : 'true', 
			'analyzer_class' : 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 
			'tokenization_enable_stemming' : 'true', 
			'tokenization_locale' : 'en', 
			'tokenization_normalize_lowercase' : 'true', 
			'tokenization_skip_stop_words' : 'true'
		};`
	if err := s.DB.Query(tableResultsIndex).Exec(); err != nil {
		return err
	}

	tableResultsIndex = `CREATE CUSTOM INDEX results_finished_date_time_idx 
		ON results (finished_date_time) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableResultsIndex).Exec(); err != nil {
		return err
	}

	tableResultsIndex = `CREATE CUSTOM INDEX results_service_name_idx 
		ON results (service_name) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableResultsIndex).Exec(); err != nil {
		return err
	}

	tableResultsIndex = `CREATE CUSTOM INDEX results_sha256_idx 
		ON results (sha256) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableResultsIndex).Exec(); err != nil {
		return err
	}

	tableResultsIndex = `CREATE CUSTOM INDEX results_started_date_time_idx 
		ON results (started_date_time) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableResultsIndex).Exec(); err != nil {
		return err
	}
//////////	
// WARNING: Uncomment only if needed. This can increase physical storage costs by ~40% with 1 million samples and 4 Services.
//	tableResultsIndex := `CREATE CUSTOM INDEX results_results_idx 
//	ON results (results) 
//	USING 'org.apache.cassandra.index.sasi.SASIIndex' 
//	WITH OPTIONS = {
//		'analyzed' : 'true', 
//		'analyzer_class' : 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 
//		'tokenization_enable_stemming' : 'false', 
//		'tokenization_locale' : 'en', 
//		'tokenization_normalize_lowercase' : 'true', 
//		'tokenization_skip_stop_words' : 'true',
//		'max_compaction_flush_memory_in_mb': '512'
//		};`
//	if err := s.DB.Query(tableResultsIndex).Exec(); err != nil {
//		return err
//	}
//////////

	// Add SASI indexes for objects
	tableObjectsIndex := `CREATE CUSTOM INDEX objects_md5_idx 
		ON objects (md5) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableObjectsIndex).Exec(); err != nil {
		return err
	}

	tableObjectsIndex = `CREATE CUSTOM INDEX objects_mime_idx 
		ON objects (mime) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex' 
		WITH OPTIONS = {
			'analyzed' : 'true', 
			'analyzer_class' : 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 
			'tokenization_enable_stemming' : 'false', 
			'tokenization_locale' : 'en', 
			'tokenization_normalize_lowercase' : 'true', 
			'tokenization_skip_stop_words' : 'true'
		};`
	if err := s.DB.Query(tableObjectsIndex).Exec(); err != nil {
		return err
	}

	// Add SASI indexes for submissions
	tableSubmissionsIndex := `CREATE CUSTOM INDEX submissions_comment_idx 
		ON submissions (comment) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex' 
		WITH OPTIONS = {
			'analyzed' : 'true', 
			'analyzer_class' : 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 
			'tokenization_enable_stemming' : 'true', 
			'tokenization_locale' : 'en', 
			'tokenization_normalize_lowercase' : 'true', 
			'tokenization_skip_stop_words' : 'true'
		};`
	if err := s.DB.Query(tableSubmissionsIndex).Exec(); err != nil {
		return err
	}

	tableSubmissionsIndex = `CREATE CUSTOM INDEX submissions_date_idx 
		ON submissions (date) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableSubmissionsIndex).Exec(); err != nil {
		return err
	}

	tableSubmissionsIndex = `CREATE CUSTOM INDEX submissions_obj_name_idx 
		ON submissions (obj_name) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex' 
		WITH OPTIONS = {
			'mode' : 'CONTAINS'
		};`
	if err := s.DB.Query(tableSubmissionsIndex).Exec(); err != nil {
		return err
	}

	tableSubmissionsIndex = `CREATE CUSTOM INDEX submissions_sha256_idx 
		ON submissions (sha256) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableSubmissionsIndex).Exec(); err != nil {
		return err
	}

	tableSubmissionsIndex = `CREATE CUSTOM INDEX submissions_source_idx 
		ON submissions (source) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableSubmissionsIndex).Exec(); err != nil {
		return err
	}

	tableSubmissionsIndex = `CREATE CUSTOM INDEX submissions_user_id_idx 
		ON submissions (user_id) 
		USING 'org.apache.cassandra.index.sasi.SASIIndex';`
	if err := s.DB.Query(tableSubmissionsIndex).Exec(); err != nil {
		return err
	}

	return nil
}

func (s StorerCassandra) StoreObject(object *storerGeneric.Object) error {
	submissions, err := s.GetSubmissionsByObject(object.SHA256)
	if err != nil {
		return err
	}

	l := len(submissions)
	if l == 0 {
		return errors.New("Tried to store an object which was never submited!")
	}

	source := make([]string, l)
	obj_name := make([]string, l)
	submission_ids := make([]string, l)
	for k, v := range submissions {
		source[k] = v.Source
		obj_name[k] = v.ObjName
		submission_ids[k] = v.Id
	}

	// just one submission implies a new object
	// more than one implies an update.
	if l == 1 {
		err = s.DB.Query(`INSERT INTO objects (sha256, sha1, md5, mime, source, obj_name, submissions) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			object.SHA256,
			object.SHA1,
			object.MD5,
			object.MIME,
			source,
			obj_name,
			submission_ids,
		).Exec()
	} else {
		err = s.DB.Query(`UPDATE objects SET source = ?,  obj_name = ?, submissions = ? WHERE sha256 = ?`,
			source,
			obj_name,
			submission_ids,
			object.SHA256,
		).Exec()
	}
	object.Source = source
	object.ObjName = obj_name
	object.Submissions = submission_ids

	return err
}

func (s StorerCassandra) GetObject(id string) (*storerGeneric.Object, error) {
	object := &storerGeneric.Object{}

	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return object, err
	}

	err = s.DB.Query(`SELECT * FROM objects WHERE sha256 = ? LIMIT 1`, uuid).Scan(
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

func (s StorerCassandra) GetSubmissionsByObject(sha256 string) ([]*storerGeneric.Submission, error) {
	submissions := []*storerGeneric.Submission{}
	submission := &storerGeneric.Submission{}

	iter := s.DB.Query(`SELECT id, sha256, user_id, source, date, obj_name, tags, comment FROM submissions WHERE sha256 = ?`, sha256).Iter()
	for iter.Scan(
		&submission.Id,
		&submission.SHA256,
		&submission.UserId,
		&submission.Source,
		&submission.Date,
		&submission.ObjName,
		&submission.Tags,
		&submission.Comment,
	) {
		submissions = append(submissions, submission)
		submission = &storerGeneric.Submission{}
	}

	err := iter.Close()

	return submissions, err
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

func (s StorerCassandra) StoreConfig(config *storerGeneric.Config) error {
	err := s.DB.Query(`INSERT INTO config (path, file_contents) VALUES (?, ?)`,
		config.Path,
		config.FileContents,
	).Exec()

	return err
}

func (s StorerCassandra) GetConfig(path string) (*storerGeneric.Config, error) {
	config := &storerGeneric.Config{}

	err := s.DB.Query(`SELECT * FROM config WHERE path = ? LIMIT 1`, path).Scan(
		&config.Path,
		&config.FileContents,
	)

	return config, err
}
