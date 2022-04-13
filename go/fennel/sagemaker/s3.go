package sagemaker

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func (smc SMClient) UploadModelToS3(file io.Reader, name string) error {
	input := s3manager.UploadInput{
		Body:   file,
		Bucket: aws.String(smc.args.S3BucketName),
		Key:    aws.String(name),
	}
	_, err := smc.s3Uploader.Upload(&input)
	if err != nil {
		return err
	}
	return nil
}

func (smc SMClient) DeleteModelFromS3(name string) error {
	input := s3.DeleteObjectInput{
		Bucket: aws.String(smc.args.S3BucketName),
		Key:    aws.String(name),
	}
	objects := []s3manager.BatchDeleteObject{{Object: &input}}
	iterator := s3manager.DeleteObjectsIterator{Objects: objects}
	err := smc.s3Deleter.Delete(aws.BackgroundContext(), &iterator)
	if err != nil {
		return err
	}
	return nil
}
