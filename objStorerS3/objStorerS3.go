package ObjStorerS3

import (
	"bytes"
	"errors"
	"io/ioutil"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/cynexit/Holmes-Storage/objStorerGeneric"
)

type ObjStorerS3 struct {
	DB     *s3.S3
	Bucket string
}

func (s ObjStorerS3) Initialize(c []*objStorerGeneric.ObjDBConnector) (objStorerGeneric.ObjStorer, error) {
	if len(c) < 1 {
		return nil, errors.New("Supply at least one node to connect to!")
	}

	s.DB = s3.New(session.New(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			c[0].Key,
			c[0].Secret,
			""),
		Endpoint:         aws.String(c[0].IP + ":" + strconv.Itoa(c[0].Port)),
		Region:           aws.String(c[0].Region),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(c[0].DisableSSL),
	}))

	s.Bucket = c[0].Bucket

	// since there is no definit way to test the connection
	// we are just doint a dummy request here to see if the
	// connection is stable
	_, err := s.DB.ListBuckets(&s3.ListBucketsInput{})
	return s, err
}

func (s ObjStorerS3) Setup() error {
	// test if the bucket already exists
	_, err := s.DB.ListObjects(&s3.ListObjectsInput{
		Bucket: &s.Bucket,
	})

	// create the bucket if it doesn't exist
	if err != nil {
		_, err := s.DB.CreateBucket(&s3.CreateBucketInput{
			Bucket: &s.Bucket,
		})

		if err != nil {
			return err
		}

		if err = s.DB.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &s.Bucket}); err != nil {
			return err
		}
	}

	return nil
}

func (s ObjStorerS3) StoreSample(sample *objStorerGeneric.Sample) error {
	_, err := s.DB.PutObject(&s3.PutObjectInput{
		Body:   bytes.NewReader(sample.Data),
		Bucket: &s.Bucket,
		Key:    &sample.SHA256,
	})

	return err
}

func (s ObjStorerS3) GetSample(id string) (*objStorerGeneric.Sample, error) {
	sample := &objStorerGeneric.Sample{SHA256: id}

	resp, err := s.DB.GetObject(&s3.GetObjectInput{
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
