package dataStorage

import (
	"time"
)

/*
This file contains structs to represent all default
collections and interfaces.
If you need to extend these for your database specific
implementation add a wrapper to your specific storer file,
don't change these structs here!
*/

type Connector struct {
	Engine   string
	IP       string
	Port     int
	User     string
	Password string
	Database string
	Secure   bool
}

type Storage interface {
	//-- General Database

	// Initializes the connection and the Storer object
	// ip, port, user, passwort, db name
	Initialize([]*Connector) error

	// Is called to setup the db on the very first run
	// to create initial collections and tables (if necessary)
	Setup() error

	Recover()

	// The functions below are abstractions of the database
	// layout Holmes is using.

	//-- Objects
	ObjectGet(sha256 string) (*Object, error)
	ObjectStore(obj *Object) (bool, error) // This function should only insert if the sample wasn't there before. The returned bool is true, if it was previously unknown.
	ObjectSearch(searchObj *Object, limit int) ([]*Object, error)
	ObjectDelete(sha256 string) error
	ObjectUpdate(sha256 string) error

	//-- Results
	ResultGet(id string) (*Result, error)
	ResultStore(res *Result) error
	ResultSearch(searchRes *Result, limit int) ([]*Result, error)
	ResultDelete(id string) error

	//-- Submissions
	SubmissionGet(id string) (*Submission, error)
	SubmissionStore(sub *Submission) error
	SubmissionSearch(searchSub *Submission, limit int) ([]*Submission, error)
	SubmissionDelete(id string) error

	//-- Config
	ConfigGet(path string) (*Config, error)
	ConfigStore(conf *Config) error
}

type Object struct {
	Id              string    `json:"id"`
	Type            string    `json:"type"`
	CreatedDateTime time.Time `json:"created_date_time"`
	Submissions     []string  `json:"submissions"`
	Source          []string  `json:"source"`

	MD5    string `json:"md5"`
	SHA1   string `json:"sha1"`
	SHA256 string `json:"sha256"`

	FileMime string   `json:"file_mime"`
	FileName []string `json:"file_name"`

	DomainFQDN      string `json:"domain_fqdn"`
	DomainTLD       string `json:"domain_tld"`
	DomainSubDomain string `json:"domain_sub_domain"`

	IPAddress string `json:"ip_address"`
	IPv6      bool   `json:"ip_v6"`

	EmailAddress       string `json:"email_address"`
	EmailLocalPart     string `json:"email_local_part"`
	EmailDomainPart    string `json:"email_domain_part"`
	EmailSubAddressing string `json:"email_address"`

	Generic_Identifier string `json:"generic_identifier"`
}

type Submission struct {
	Id       string    `json:"id"`
	SHA256   string    `json:"sha256"`
	UserId   string    `json:"user_id"`
	Source   string    `json:"source"`
	DateTime time.Time `json:"date_time"`
	ObjName  string    `json:"obj_name"`
	Tags     []string  `json:"tags"`
	Comment  string    `json:"comment"`
}

type Result struct {
	Id                string    `json:"id"`
	SHA256            string    `json:"sha256"`
	SchemaVersion     string    `json:"schema_version"`
	UserId            string    `json:"user_id"`
	SourceId          []string  `json:"source_id"`
	SourceTag         []string  `json:"source_tag"`
	ServiceName       string    `json:"service_name"`
	ServiceVersion    string    `json:"service_version"`
	ServiceConfig     string    `json:"service_config"`
	ObjectCategory    []string  `json:"object_category"`
	ObjectType        string    `json:"object_type"`
	Results           string    `json:"results"`
	Tags              []string  `json:"tags"`
	StartedDateTime   time.Time `json:"started_date_time"`
	FinishedDateTime  time.Time `json:"finished_date_time"`
	WatchguardStatus  string    `json:"watchguard_status"`
	WatchguardLog     []string  `json:"watchguard_log"`
	WatchguardVersion string    `json:"watchguard_version"`
}

type Config struct {
	Path         string `json:"path"`
	FileContents string `json:"file_contents"`
}
