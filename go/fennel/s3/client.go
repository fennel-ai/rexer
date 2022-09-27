package s3

import (
	"context"
	"fennel/lib/timer"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Args struct {
	Region string `arg:"--region,env:AWS_REGION,help:AWS region"`
}

type Client struct {
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	deleter    *s3manager.BatchDelete
	client     *s3.S3
}

func NewClient(args S3Args) Client {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:                        aws.String(args.Region),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	))
	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)
	deleter := s3manager.NewBatchDelete(sess)
	client := s3.New(sess)
	return Client{
		uploader:   uploader,
		downloader: downloader,
		deleter:    deleter,
		client:     client,
	}
}

func (c Client) ListFiles(bucketName, pathPrefix, continuationToken string) ([]string, error) {
	_, t := timer.Start(context.Background(), 0, "s3client.ListFiles")
	defer t.Stop()
	input := s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(pathPrefix),
		MaxKeys: aws.Int64(1000),
	}

	if continuationToken != "" {
		input.ContinuationToken = aws.String(continuationToken)
	}

	output, err := c.client.ListObjectsV2(&input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchBucket {
				return []string{}, nil
			}
		}
		return nil, err
	}

	var files []string
	for _, obj := range output.Contents {
		files = append(files, *obj.Key)
	}

	if *output.IsTruncated {
		additionalFiles, err := c.ListFiles(bucketName, pathPrefix, *output.NextContinuationToken)
		if err != nil {
			return nil, err
		}
		files = append(files, additionalFiles...)
	}
	return files, nil
}

func (c Client) Upload(file io.Reader, path, bucketName string) error {
	_, t := timer.Start(context.Background(), 0, "s3client.Upload")
	defer t.Stop()
	input := s3manager.UploadInput{
		Body:   file,
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}
	_, err := c.uploader.Upload(&input)
	return err
}

func (c Client) Exists(path string, bucketName string) (bool, error) {
	_, err := c.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
				return false, nil
			default:
				return false, err
			}
		}
		return false, err
	}
	return true, nil
}

func (c Client) Download(path, bucketName string) ([]byte, error) {
	_, t := timer.Start(context.Background(), 0, "s3client.Download")
	defer t.Stop()
	input := s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}
	buf := aws.WriteAtBuffer{}
	_, err := c.downloader.Download(&buf, &input)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c Client) Delete(path string, bucketName string) error {
	input := s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}
	objects := []s3manager.BatchDeleteObject{{Object: &input}}
	iterator := s3manager.DeleteObjectsIterator{Objects: objects}
	return c.deleter.Delete(aws.BackgroundContext(), &iterator)
}

// srcPath is the entire path include the bucket name eg s3://bucket/path/to/file ->b srcPath = bucket/path/to/file
// dstPath is the suffix of the path (excluding the bucket name) eg s3://bucket/path/to/file -> dstPath = path/to/file
func (c Client) CopyFile(srcPath, dstPath, dstBucketName string) error {
	_, t := timer.Start(context.Background(), 0, "s3client.CopyFile")
	defer t.Stop()
	// Check if bucket exists
	input := s3.HeadBucketInput{
		Bucket: aws.String(dstBucketName),
	}
	_, err := c.client.HeadBucket(&input)

	// If bucket does not exist, create it
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() != "NotFound" {
				return err
			}
		}
		// Create S3 bucket
		input := s3.CreateBucketInput{
			Bucket: aws.String(dstBucketName),
		}
		_, err = c.client.CreateBucket(&input)
		if err != nil {
			return err
		}
	}

	copyInput := s3.CopyObjectInput{
		Bucket:     aws.String(dstBucketName),
		CopySource: aws.String(srcPath),
		Key:        aws.String(dstPath),
	}
	_, err = c.client.CopyObject(&copyInput)
	return err
}

// Downloads the s3 files to the folder specified with the same file names as in the s3 bucket
func (c Client) BatchDiskDownload(paths []string, bucketName string, folderName string) error {
	_, t := timer.Start(context.Background(), 0, "s3client.BatchDiskDownload")
	defer t.Stop()
	fileWriters := make([]*os.File, len(paths))
	var err error
	for i, path := range paths {
		pathArray := strings.Split(path, "/")
		fileWriters[i], err = os.Create(folderName + "/" + pathArray[len(pathArray)-1])
		if err != nil {
			return err
		}
	}

	objects := make([]s3manager.BatchDownloadObject, len(paths))
	for i, path := range paths {
		objects[i] = s3manager.BatchDownloadObject{
			Object: &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(path),
			},
			Writer: fileWriters[i],
		}
	}

	iter := &s3manager.DownloadObjectsIterator{Objects: objects}
	if err := c.downloader.DownloadWithIterator(aws.BackgroundContext(), iter); err != nil {
		return err
	}
	return nil
}
