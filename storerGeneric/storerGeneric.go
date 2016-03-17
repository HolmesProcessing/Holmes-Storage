package storerGeneric

/*
This file contains structs to represent all default
collections and interfaces.
If you need to extend these for your database specific
implementation add a wrapper to your specific storer.../*.go file,
don't change these structs here!
*/

type DBConnector struct {
	IP       string
	Port     int
	User     string
	Password string
	Database string
}

type Storer interface {
	// Initializes the connection and the Storer object
	// ip, port, user, passwort, db name
	Initialize([]*DBConnector) (Storer, error)

	// Is called to setup the db on the very first run
	// to create initial collections (if necessary)
	Setup() error

	StoreObject(*Object) error
	GetObject(string) (*Object, error)

	StoreSubmission(*Submission) error
	GetSubmission(string) (*Submission, error)

	// Stores a new sample in the database
	// return "duplicate" error if already known
	StoreSample(*Sample) error

	// Gets a sample from the database, identified
	// by its sha2 string
	GetSample(string) (*Sample, error)

	// Stores a result in the database
	// (TODO: return generated Id)
	StoreResult(*Result) error

	// Gets a result by Id from the database
	GetResult(string) (*Result, error)
}

type Object struct {
	SHA256      string   `json:"sha256"`
	SHA1        string   `json:"sha1"`
	MD5         string   `json:"md5"`
	MIME        string   `json:"mime"`
	Source      []string `json:"source"`
	ObjName     []string `json:"obj_name"`
	Submissions []string `json:"submissions"`
}

type Submission struct {
	Id      string   `json:"id"`
	SHA256  string   `json:"sha256"`
	UserId  string   `json:"user_id"`
	Source  string   `json:"source"`
	Date    string   `json:"date"`
	ObjName string   `json:"obj_name"`
	Tags    []string `json:"tags"`
	Comment string   `json:"comment"`
}

type Sample struct {
	SHA256 string `json:"sha256"`
	Data   []byte `json:"data"` //this will result in a base64 encoded string when marshaled
}

type Result struct {
	Id                string   `json:"id"`
	SHA256            string   `json:"sha256"`
	SchemaVersion     string   `json:"schema_version"`
	UserId            string   `json:"user_id"`
	SourceId          []string `json:"source_id"`
	SourceTag         []string `json:"source_tag"`
	ServiceName       string   `json:"service_name"`
	ServiceVersion    string   `json:"service_version"`
	ServiceConfig     string   `json:"service_config"`
	ObjectCategory    string   `json:"object_category"`
	ObjectType        string   `json:"object_type"`
	Results           string   `json:"results"`
	Tags              []string `json:"tags"`
	StartedDateTime   string   `json:"started_date_time"`
	FinishedDateTime  string   `json:"finished_date_time"`
	WatchguardStatus  string   `json:"watchguard_status"`
	WatchguardLog     []string `json:"watchguard_log"`
	WatchguardVersion string   `json:"watchguard_version"`
}
