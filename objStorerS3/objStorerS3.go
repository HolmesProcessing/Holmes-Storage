package ObjStorerS3

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/cynexit/Holmes-Storage/objStorerGeneric"
	"github.com/cynexit/Holmes-Storage/storerGeneric"
)

type ObjStorerS3 struct {
	DB *s3.S3
	Bucket 		string
}

func (s ObjStorerS3) Initialize(c []*storerGeneric.DBConnector) (objStorerGeneric.ObjStorer, error) {
	if len(c) < 1 {
		return nil, errors.New("Supply at least one node to connect to!")
	}

	auth, err := aws.Creds(c[0].Key, c[0].Secret, "")
	if err != nil {
		return nil, errors.New("Please supply a Key/Secret to use!")
	}

	s.DB := s3.New(&aws.Config{
		Credentials: 	  auth,
		Endpoint:         aws.String("192.168.45.42:8080"), // Where Riak CS was running (via docker)
		Region:           aws.String(c[0].Region),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(c[0].DisableSSL),
	})

	if err := svc.ListBuckets(&s3.ListBucketsInput{}); err != nil {
		return err
	}

	return s, err
}

func (s ObjStorerS3) Setup() error {
	//set bucket variable
	s.Bucket := aws.String(c[0].Bucket)

	// test if the bucket already exists
	exists, err := s.DB.ListObjects(&s3.ListObjectsInput{
		Bucket: &s.Bucket,
	})
	if err != nil {
		return err
	}

	// create the bucket if it doesn't exist
	if exists == nil {
		result, err := s.DB.CreateBucket(&s3.CreateBucketInput{
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
	// TODO: Check to see if already known

	// TODO: check to make sure the data is being sent in the proper format. looks like the generic is json
	// and I cannot remember the best practices to stream in golang
	uploadResult, err = s.DB.PutObject(&s3.PutObjectInput{
		Body:   &sample.Data,
		Bucket: &s.Bucket,
		Key:    &sample.SHA256,
	})
	if err != nil {
		return errors.New("Failed to upload data to %s/%s, %s\n", s.Bucket, sample.SHA256, err)
	}

	return err
}

func (s ObjStorerS3) GetSample(id string) (*objStorerGeneric.Sample, error) {

	sample := &storerGeneric.Sample{}

	out, err := s.DB.GetObject(&s3.GetObjectInput{
		Bucket: &s.Bucket
		Key:    id,
	})

	&sample.Data = out.Body
	&sample.SHA256 = id

	return sample, err
}

// TODO: Support MultipleObjects retrieval and getting. Useful when using something over 100megs
