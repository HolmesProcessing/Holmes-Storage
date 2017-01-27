package dataStorage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	//TODO: Take a look at gocassa, gocqltable, cqlc, cqlr
	//      and check if these packages would be a good addition.
)

type Cassandra struct {
	DB *gocql.Session
}

var (
	recoverLock    = &sync.RWMutex{}
	connectionData []*Connector
)

func (s *Cassandra) Initialize(c []*Connector) error {
	connectionData = c

	if len(c) < 1 {
		return errors.New("Supply at least one node to connect to!")
	}

	connStrings := make([]string, len(c))
	for i, elem := range c {
		connStrings[i] = fmt.Sprintf("%s:%d", elem.IP, elem.Port)
	}

	if c[0].Database == "" {
		return errors.New("Please supply a database/keyspace to use!")
	}

	cluster := gocql.NewCluster(connStrings...)
	cluster.Keyspace = c[0].Database
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: c[0].User,
		Password: c[0].Password,
	}
	cluster.ProtoVersion = 4

	var err error
	s.DB, err = cluster.CreateSession()

	return err
}

func (s *Cassandra) Setup() error {
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
	// spaces needed, !NO TABS!
	tableResults := `CREATE TABLE results(
        id timeuuid,
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
        execution_time time,
        watchguard_status text,
        watchguard_log list<text>,
        watchguard_version text,
        comment text,
    PRIMARY KEY ((service_name, object_type), id, service_version))
    WITH CLUSTERING ORDER BY (id DESC)
    AND compression = { 
        'enabled': 'true', 
        'class' : 'LZ4Compressor' 
    };`
	if err := s.DB.Query(tableResults).Exec(); err != nil {
		return err
	}

	tableResultsBySHA256 := `CREATE MATERIALIZED VIEW results_by_sha256 AS
        SELECT * FROM results
        WHERE sha256 IS NOT NULL 
        AND id IS NOT NULL 
        AND service_name IS NOT NULL 
        AND service_version IS NOT NULL 
        AND object_type IS NOT NULL
        PRIMARY KEY((sha256), id, service_name, service_version, object_type)
        WITH CLUSTERING ORDER BY (id DESC);`
	if err := s.DB.Query(tableResultsBySHA256).Exec(); err != nil {
		return err
	}

	tableObjects := `CREATE TABLE objects(
        type text,
        creation_date_time timestamp,
        submissions set<timeuuid>,
        source set<text>,

        md5 text,
        sha1 text,
        sha256 text,

        file_mime text,
        file_name set<text>,

        domain_fqdn text,
        domain_tld text,
        domain_sub_domain text,

        ip_address inet,
        ip_v6 boolean,

        email_address text,
        email_local_part text,
        email_domain_part text,
        email_sub_addressing text,

        generic_identifier text,
        generic_type text,
        generic_data_rel_address text,
    PRIMARY KEY ((sha256), creation_date_time, type))
    WITH CLUSTERING ORDER BY (creation_date_time DESC)
    AND compression = { 
        'enabled': 'true', 
        'class' : 'LZ4Compressor' 
    };`
	if err := s.DB.Query(tableObjects).Exec(); err != nil {
		return err
	}

	tableObjectsByTypeFile := `CREATE MATERIALIZED VIEW objects_by_type_file AS
        SELECT creation_date_time, submissions, source, md5, sha1, sha256, file_mime, file_name
        FROM objects
        WHERE file_mime IS NOT NULL
        AND creation_date_time IS NOT NULL
        AND sha256 IS NOT NULL
        AND type = 'file'
        PRIMARY KEY((file_mime), creation_date_time, sha256, type)
        WITH CLUSTERING ORDER BY (creation_date_time DESC);`
	if err := s.DB.Query(tableObjectsByTypeFile).Exec(); err != nil {
		return err
	}

	tableSubmissions := `CREATE TABLE submissions(
        id timeuuid,
        sha256 text,
        user_id text,
        source text,
        date_time timestamp,
        obj_name text,
        tags set<text>,
        comment text,
    PRIMARY KEY ((sha256), id))
    WITH CLUSTERING ORDER BY (id DESC)
    AND compression = { 
        'enabled': 'true', 
        'class' : 'LZ4Compressor'
    };`
	if err := s.DB.Query(tableSubmissions).Exec(); err != nil {
		return err
	}
	tableSubmissionsByUser := `CREATE MATERIALIZED VIEW submissions_by_user_id
        AS SELECT *
        FROM submissions
        WHERE user_id IS NOT NULL 
        AND id IS NOT NULL 
        AND sha256 IS NOT NULL
        PRIMARY KEY((user_id), id, sha256)
        WITH CLUSTERING ORDER BY (id desc);`
	if err := s.DB.Query(tableSubmissionsByUser).Exec(); err != nil {
		return err
	}
	tableSubmissionsBySource := `CREATE MATERIALIZED VIEW submissions_by_source
        AS SELECT *
        FROM submissions
        WHERE source IS NOT NULL 
        AND id IS NOT NULL 
        AND sha256 IS NOT NULL
        PRIMARY KEY((source), id, sha256)
        WITH CLUSTERING ORDER BY (id desc);`
	if err := s.DB.Query(tableSubmissionsBySource).Exec(); err != nil {
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

	return nil
}

func (s *Cassandra) Recover() {
	recoverLock.Lock()

	var err error

	err = s.Initialize(connectionData)
	for err != nil {
		time.Sleep(time.Second * 5)
		fmt.Println("Trying to recover broken Cassandra connection...")
		err = s.Initialize(connectionData)
	}

	recoverLock.Unlock()
}

func (s *Cassandra) ObjectGet(sha256 string) (object *Object, err error) {
	defer func() {
		recoverLock.RUnlock()

		if r := recover(); r != nil {
			s.Recover()
			object, err = s.ObjectGet(sha256)
		}
	}()

	object = &Object{}

	recoverLock.RLock()
	err = s.DB.Query("SELECT type, creation_date_time, submissions, source, md5, sha1, sha256, file_mime, file_name, domain_fqdn, domain_tld, domain_sub_domain, ip_address, ip_v6, email_address, email_local_part, email_domain_part, email_sub_addressing, generic_identifier, generic_type, generic_data_rel_address FROM objects WHERE sha256 = ? LIMIT 1", sha256).Scan(
		&object.Type,
		&object.CreationDateTime,
		&object.Submissions,
		&object.Source,
		&object.MD5,
		&object.SHA1,
		&object.SHA256,
		&object.FileMime,
		&object.FileName,
		&object.DomainFQDN,
		&object.DomainTLD,
		&object.DomainSubDomain,
		&object.IPAddress,
		&object.IPv6,
		&object.EmailAddress,
		&object.EmailLocalPart,
		&object.EmailDomainPart,
		&object.EmailSubAddressing,
		&object.GenericIdentifier,
		&object.GenericType,
		&object.GenericDataRelAddress,
	)

	if err == gocql.ErrTimeoutNoResponse {
		panic("connection broke")
	}

	return object, err
}

func (s *Cassandra) ObjectStore(obj *Object) (bool, error) {
	submissions, err := s.SubmissionsGetByObject(obj.SHA256)
	if err != nil {
		return false, err
	}

	l := len(submissions)
	if l == 0 {
		return false, errors.New("Tried to store an object which was never submited!")
	}

	source := make([]string, l)
	file_name := make([]string, l)
	submission_ids := make([]string, l)
	for k, v := range submissions {
		source[k] = v.Source
		file_name[k] = v.ObjName
		submission_ids[k] = v.Id
	}
	inserted := false
	// just one submission implies a new object
	// more than one implies an update.
	if l == 1 {
		inserted = true
		err = s.DB.Query("INSERT INTO objects (type, creation_date_time, submissions, source, md5, sha1, sha256, file_mime, file_name, domain_fqdn, domain_tld, domain_sub_domain, ip_address, ip_v6, email_address, email_local_part, email_domain_part, email_sub_addressing, generic_identifier, generic_type, generic_data_rel_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			obj.Type,
			obj.CreationDateTime,
			obj.Submissions,
			obj.Source,
			obj.MD5,
			obj.SHA1,
			obj.SHA256,
			obj.FileMime,
			obj.FileName,
			obj.DomainFQDN,
			obj.DomainTLD,
			obj.DomainSubDomain,
			obj.IPAddress,
			obj.IPv6,
			obj.EmailAddress,
			obj.EmailLocalPart,
			obj.EmailDomainPart,
			obj.EmailSubAddressing,
			obj.GenericIdentifier,
			obj.GenericType,
			obj.GenericDataRelAddress,
		).Exec()
	} else {
		err = s.DB.Query(`UPDATE objects SET source = ?,  file_name = ?, submissions = ? WHERE sha256 = ?`,
			source,
			file_name,
			submission_ids,
			obj.SHA256,
		).Exec()
	}

	obj.Source = source
	obj.FileName = file_name
	obj.Submissions = submission_ids

	return inserted, err
}

func (s *Cassandra) ObjectSearch(searchObj *Object, limit int) ([]*Object, error) {
	return nil, errors.New("Not implemented")
}

func (s *Cassandra) ObjectDelete(sha256 string) error {
	return s.DB.Query(`DELETE FROM objects WHERE sha256 = ?`, sha256).Exec()
}

func (s *Cassandra) ObjectUpdate(sha256 string) error {
	return errors.New("Not implemented")
}

func (s *Cassandra) updateSubmissions(sha256 string) error {
	submissions, err := s.SubmissionsGetByObject(sha256)
	if err != nil {
		return err
	}

	l := len(submissions)
	if l == 0 {
		// TODO: remove the entry, if it exists?
		return errors.New("Tried to update an object which was never submited!")
	}

	source := make([]string, l)
	obj_name := make([]string, l)
	submission_ids := make([]string, l)
	for k, v := range submissions {
		source[k] = v.Source
		obj_name[k] = v.ObjName
		submission_ids[k] = v.Id
	}

	err = s.DB.Query(`UPDATE objects SET source = ?,  obj_name = ?, submissions = ? WHERE sha256 = ?`,
		source,
		obj_name,
		submission_ids,
		sha256,
	).Exec()
	return err
}

func (s *Cassandra) ResultGet(id string) (*Result, error) {
	result := &Result{}

	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return result, err
	}

	err = s.DB.Query("SELECT id, sha256, schema_version, user_id, source_id, source_tag, service_name, service_version, service_config, object_category, object_type, results, tags, execution_time, watchguard_status, watchguard_log, watchguard_version, comment FROM results WHERE id = ? LIMIT 1", uuid).Scan(
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
		&result.ExecutionTime,
		&result.WatchguardStatus,
		&result.WatchguardLog,
		&result.WatchguardVersion,
		&result.Comment,
	)

	return result, err
}

func (s *Cassandra) ResultStore(res *Result) error {
	id := gocql.TimeUUID()

	err := s.DB.Query("INSERT INTO results (id, sha256, schema_version, user_id, source_id, source_tag, service_name, service_version, service_config, object_category, object_type, results, tags, execution_time, watchguard_status, watchguard_log, watchguard_version, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		id,
		res.SHA256,
		res.SchemaVersion,
		res.UserId,
		res.SourceId,
		res.SourceTag,
		res.ServiceName,
		res.ServiceVersion,
		res.ServiceConfig,
		res.ObjectCategory,
		res.ObjectType,
		res.Results,
		res.Tags,
		res.ExecutionTime,
		res.WatchguardStatus,
		res.WatchguardLog,
		res.WatchguardVersion,
		res.Comment,
	).Exec()

	return err
}

func (s *Cassandra) ResultSearch(searchRes *Result, limit int) ([]*Result, error) {
	return nil, errors.New("Not implemented")
}

func (s *Cassandra) ResultDelete(id string) error {
	return s.DB.Query(`DELETE FROM results WHERE id = ?`, id).Exec()
}

func (s *Cassandra) SubmissionGet(id string) (submission *Submission, err error) {
	defer func() {
		recoverLock.RUnlock()

		if r := recover(); r != nil {
			s.Recover()
			submission, err = s.SubmissionGet(id)
		}
	}()

	submission = &Submission{}

	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return submission, err
	}

	recoverLock.RLock()
	err = s.DB.Query("SELECT id, sha256, user_id, source, date_time, obj_name, tags, comment FROM submissions WHERE id = ? LIMIT 1", uuid).Scan(
		&submission.Id,
		&submission.SHA256,
		&submission.UserId,
		&submission.Source,
		&submission.DateTime,
		&submission.ObjName,
		&submission.Tags,
		&submission.Comment,
	)

	if err == gocql.ErrTimeoutNoResponse {
		panic("connection broke")
	}

	return submission, err
}

func (s *Cassandra) SubmissionStore(sub *Submission) error {
	id, err := gocql.RandomUUID()
	if err != nil {
		return err
	}

	sub.Id = id.String()

	err = s.DB.Query("INSERT INTO submissions (id, sha256, user_id, source, date_time, obj_name, tags, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		id,
		sub.SHA256,
		sub.UserId,
		sub.Source,
		sub.DateTime,
		sub.ObjName,
		sub.Tags,
		sub.Comment,
	).Exec()

	return err
}

func (s *Cassandra) SubmissionSearch(searchSub *Submission, limit int) ([]*Submission, error) {
	return nil, errors.New("Not implemented")
}

func (s *Cassandra) SubmissionDelete(id string) error {
	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return err
	}

	err = s.DB.Query(`DELETE FROM submissions WHERE id = ?`, uuid).Exec()

	return err
}

func (s *Cassandra) SubmissionsGetByObject(sha256 string) ([]*Submission, error) {
	submissions := []*Submission{}
	submission := &Submission{}

	iter := s.DB.Query("SELECT id, sha256, user_id, source, date_time, obj_name, tags, comment FROM submissions WHERE sha256 = ?", sha256).Iter()
	for iter.Scan(
		&submission.Id,
		&submission.SHA256,
		&submission.UserId,
		&submission.Source,
		&submission.DateTime,
		&submission.ObjName,
		&submission.Tags,
		&submission.Comment,
	) {
		submissions = append(submissions, submission)
		submission = &Submission{}
	}

	err := iter.Close()

	return submissions, err
}

func (s *Cassandra) ConfigGet(path string) (*Config, error) {
	config := &Config{}

	err := s.DB.Query(`SELECT * FROM config WHERE path = ? LIMIT 1`, path).Scan(
		&config.Path,
		&config.FileContents,
	)

	return config, err
}

func (s *Cassandra) ConfigStore(config *Config) error {
	err := s.DB.Query(`INSERT INTO config (path, file_contents) VALUES (?, ?)`,
		config.Path,
		config.FileContents,
	).Exec()

	return err
}
