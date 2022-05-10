package s3

import (
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
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

func (c Client) ListFiles(bucketName, pathPrefix string) ([]string, error) {
	input := s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(pathPrefix),
		MaxKeys: aws.Int64(10000),
	}
	output, err := c.client.ListObjectsV2(&input)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, obj := range output.Contents {
		files = append(files, *obj.Key)
	}
	return files, nil
}

func (c Client) Upload(file io.Reader, path, bucketName string) error {
	input := s3manager.UploadInput{
		Body:   file,
		Bucket: aws.String(bucketName),
		Key:    aws.String(path),
	}
	_, err := c.uploader.Upload(&input)
	return err
}

func (c Client) Download(path, bucketName string) ([]byte, error) {
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

// Downloads the s3 files to the folder specified with the same file names as in the s3 bucket
func (c Client) BatchDiskDownload(paths []string, bucketName string, folderName string) error {
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
