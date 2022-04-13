package sagemaker

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func (smc SMClient) GetArtifactPath(fileName string) string {
	return fmt.Sprintf("s3://%s/%s", smc.args.S3BucketName, fileName)
}

func (smc SMClient) UploadModelToS3(file io.Reader, fileName string) error {
	input := s3manager.UploadInput{
		Body:   file,
		Bucket: aws.String(smc.args.S3BucketName),
		Key:    aws.String(fileName),
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
