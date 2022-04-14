package s3

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Args struct {
	Region string `arg:"--region,env:AWS_REGION,help:AWS region"`
}

type Client struct {
	args     S3Args
	uploader *s3manager.Uploader
	deleter  *s3manager.BatchDelete
}

func NewClient(args S3Args) Client {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:                        aws.String(args.Region),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	))
	uploader := s3manager.NewUploader(sess)
	deleter := s3manager.NewBatchDelete(sess)
	return Client{
		uploader: uploader,
		deleter:  deleter,
	}
}

func (c Client) UploadModelToS3(file io.Reader, fileName, bucketName string) error {
	input := s3manager.UploadInput{
		Body:   file,
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
	}
	_, err := c.uploader.Upload(&input)
	return err
}

func (c Client) DeleteModelFromS3(fileName string, bucketName string) error {
	input := s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
	}
	objects := []s3manager.BatchDeleteObject{{Object: &input}}
	iterator := s3manager.DeleteObjectsIterator{Objects: objects}
	return c.deleter.Delete(aws.BackgroundContext(), &iterator)
}
