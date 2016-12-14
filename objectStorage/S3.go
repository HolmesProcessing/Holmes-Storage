package objectStorage

import (
	"bytes"
	"errors"
	"io/ioutil"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	amazons3 "github.com/aws/aws-sdk-go/service/s3"
)

type S3 struct {
	DB     *amazons3.S3
	Bucket string
}

func (s S3) Initialize(c []*Connector) error {
	if len(c) < 1 {
		return errors.New("Supply at least one node to connect to!")
	}

	s3sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			c[0].Key,
			c[0].Secret,
			""),
		Endpoint:         aws.String(c[0].IP + ":" + strconv.Itoa(c[0].Port)),
		Region:           aws.String(c[0].Region),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(!c[0].Secure),
	})

	if err != nil {
		return err //same as Must(...)
	}

	s.DB = amazons3.New(s3sess)
	s.Bucket = c[0].Bucket

	// since there is no definit way to test the connection
	// we are just doint a dummy request here to see if the
	// connection is stable
	_, err = s.DB.ListBuckets(&amazons3.ListBucketsInput{})
	return err
}

func (s S3) Setup() error {
	// test if the bucket already exists
	_, err := s.DB.ListObjects(&amazons3.ListObjectsInput{
		Bucket: &s.Bucket,
	})

	// create the bucket if it doesn't exist
	if err != nil {
		_, err := s.DB.CreateBucket(&amazons3.CreateBucketInput{
			Bucket: &s.Bucket,
		})

		if err != nil {
			return err
		}

		if err = s.DB.WaitUntilBucketExists(&amazons3.HeadBucketInput{Bucket: &s.Bucket}); err != nil {
			return err
		}
	}

	return nil
}

func (s S3) SampleDelete(sample *Sample) error {
	_, err := s.DB.DeleteObject(&amazons3.DeleteObjectInput{
		Bucket: &s.Bucket,
		Key:    &sample.SHA256,
	})

	return err
}

func (s S3) SampleStore(sample *Sample) error {
	_, err := s.DB.PutObject(&amazons3.PutObjectInput{
		Body:   bytes.NewReader(sample.Data),
		Bucket: &s.Bucket,
		Key:    &sample.SHA256,
	})

	return err
}

func (s S3) SampleGet(id string) (*Sample, error) {
	sample := &Sample{SHA256: id}

	resp, err := s.DB.GetObject(&amazons3.GetObjectInput{
		Bucket: &s.Bucket,
		Key:    &id,
	})

	if err != nil {
		return sample, err
	}

	if sample.Data, err = ioutil.ReadAll(resp.Body); err != nil {
		return sample, err
	}

	return sample, err
}

// TODO: Support MultipleObjects retrieval and getting. Useful when using something over 100megs
