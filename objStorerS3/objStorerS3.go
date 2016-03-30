package ObjStorerS3

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"

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

	auth, err := aws.GetAuth(c[0].Key, c[0].Secret)
	if err != nil {
		return nil, errors.New("Please supply a Key/Secret to use!")
	}

	s.DB := s3.New(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			c[0].Key,
			c[0].Secret,
			""),
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
	bucket := c[0].Bucket)

	// test if the bucket already exists
	exists, err := s.DB.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(&bucket),
	})
	if err != nil {
		return err
	}

	// create the bucket if it doesn't exist
	if exists == nil {
		result, err := s.DB.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(&bucket),
		})
		if err != nil {
			return err
		}

		if err = s.DB.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &bucket}); err != nil {
			return err
		}
	}

	return nil
}

func (s ObjStorerS3) StoreSample(*objStorerGeneric.Sample) error {
	return nil
}

func (s ObjStorerS3) GetSample(string) (*objStorerGeneric.Sample, error) {
	return nil, nil
}
