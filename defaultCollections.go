package main

/*
This file contains structs to represent all default
collections.
If you need to extend these for your database specific
implementation add a wrapper to your storer*.go file,
don't change these structs here!
*/

type dbObjects struct {
	SHA256 string `json:"sha256"`
	SHA1   string `json:"sha1"`
	MD5    string `json:"md5"`
}

type dbSubmissions struct {
	Id     string `json:"_id"`
	SHA256 string `json:"sha256"`
	UserId int    `json:"user_id"`
	Source string `json:"source"`
	Name   string `json:"name"`
	Date   string `json:"date"`
}

type dbSamples struct {
	SHA256 string `json:"sha256"`
	Data   []byte `json:"data"` //this will result in a base64 encoded string when marshaled
}

type dbResults struct {
	Id                string                 `json:"_id"`
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
