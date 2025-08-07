package storage

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type BucketType int

const (
	BucketTypeS3 BucketType = iota
	BucketTypeMinio
	BucketTypeHTTP
)

// S3-compatible bucket
type Bucket struct {
	URL       string
	Name      string
	Endpoint  string
	Region    string
	Type      BucketType
	client    *http.Client
	s3Client  *s3.S3
	available bool
}

func NewBucket(bucketURL string) (*Bucket, error) {
	bucket := &Bucket{
		URL:       bucketURL,
		available: true,
		client:    &http.Client{Timeout: 10 * time.Second},
	}

	// Parse the URL to determine the bucket type and name
	if strings.HasPrefix(bucketURL, "s3://") {
		// AWS S3 bucket
		bucket.Type = BucketTypeS3
		bucket.Name = strings.TrimPrefix(bucketURL, "s3://")
		bucket.Region = "us-east-1" // Default region
		bucket.Endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", bucket.Region)
	} else if strings.HasPrefix(bucketURL, "http://") || strings.HasPrefix(bucketURL, "https://") {
		// HTTP bucket (could be Minio or S3)
		bucket.Type = BucketTypeHTTP
		parsedURL, err := url.Parse(bucketURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse bucket URL: %w", err)
		}

		// Extract the bucket name from the path
		pathParts := strings.Split(strings.TrimPrefix(parsedURL.Path, "/"), "/")
		if len(pathParts) == 0 || pathParts[0] == "" {
			return nil, fmt.Errorf("invalid bucket URL: %s", bucketURL)
		}
		bucket.Name = pathParts[0]
		bucket.Endpoint = fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Host)

		// Check if this is a Minio bucket
		if strings.Contains(parsedURL.Host, "minio") || strings.Contains(parsedURL.Host, "localhost") || strings.Contains(parsedURL.Host, "127.0.0.1") {
			bucket.Type = BucketTypeMinio
		}
	} else {
		return nil, fmt.Errorf("unsupported bucket URL format: %s", bucketURL)
	}

	// Initialize S3 client if needed
	if bucket.Type == BucketTypeS3 || bucket.Type == BucketTypeMinio {
		// For Minio, we can use any region
		if bucket.Type == BucketTypeMinio {
			bucket.Region = "us-east-1" // Minio doesn't care about region
		}

		var awsConfig *aws.Config

		// For Minio, use actual credentials for testing
		if bucket.Type == BucketTypeMinio {
			awsConfig = &aws.Config{
				Region:           aws.String(bucket.Region),
				Endpoint:         aws.String(bucket.Endpoint),
				S3ForcePathStyle: aws.Bool(true),
				Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
				DisableSSL:       aws.Bool(strings.HasPrefix(bucket.Endpoint, "http://")),
			}
		} else {
			// For S3, use anonymous credentials as intended
			awsConfig = &aws.Config{
				Region:           aws.String(bucket.Region),
				Endpoint:         aws.String(bucket.Endpoint),
				S3ForcePathStyle: aws.Bool(true),
				Credentials:      credentials.AnonymousCredentials,
				DisableSSL:       aws.Bool(strings.HasPrefix(bucket.Endpoint, "http://")),
			}
		}

		sess, err := session.NewSession(awsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create AWS session: %w", err)
		}

		bucket.s3Client = s3.New(sess)
	}

	return bucket, nil
}

func (b *Bucket) PutObject(key string, data []byte) (string, error) {
	fmt.Printf("PutObject called for key %s in bucket %s with %d bytes\n", key, b.Name, len(data))
	if !b.available {
		fmt.Printf("Bucket %s is not available\n", b.Name)
		return "", fmt.Errorf("bucket is not available")
	}

	if b.Type == BucketTypeS3 || b.Type == BucketTypeMinio {
		fmt.Printf("Using AWS SDK to put object in bucket %s with type %v\n", b.Name, b.Type)
		input := &s3.PutObjectInput{
			Bucket: aws.String(b.Name),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		}

		// Use unsigned request
		fmt.Printf("Creating unsigned PutObject request\n")
		req, _ := b.s3Client.PutObjectRequest(input)
		req.Config.Credentials = credentials.AnonymousCredentials
		req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		fmt.Printf("Sending PutObject request\n")
		err := req.Send()
		if err != nil {
			fmt.Printf("Failed to put object: %v\n", err)
			return "", fmt.Errorf("failed to put object: %w", err)
		}
		fmt.Printf("PutObject request successful\n")

		fmt.Printf("Getting ETag for object\n")
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(b.Name),
			Key:    aws.String(key),
		}

		// Use unsigned request
		fmt.Printf("Creating unsigned HeadObject request\n")
		headReq, headOutput := b.s3Client.HeadObjectRequest(headInput)
		headReq.Config.Credentials = credentials.AnonymousCredentials
		headReq.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		fmt.Printf("Sending HeadObject request\n")
		err = headReq.Send()
		if err != nil {
			fmt.Printf("Failed to get object metadata: %v\n", err)
			return "", fmt.Errorf("failed to get object metadata: %w", err)
		}
		fmt.Printf("HeadObject request successful\n")

		etag := strings.Trim(*headOutput.ETag, "\"")
		fmt.Printf("Got ETag: %s\n", etag)
		return etag, nil
	} else if b.Type == BucketTypeHTTP {
		url := fmt.Sprintf("%s/%s/%s", b.Endpoint, b.Name, key)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
		if err != nil {
			return "", fmt.Errorf("failed to create HTTP request: %w", err)
		}

		resp, err := b.client.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to put object: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
			return "", fmt.Errorf("failed to put object: %s", resp.Status)
		}

		headReq, err := http.NewRequest(http.MethodHead, url, nil)
		if err != nil {
			return "", fmt.Errorf("failed to create HTTP request: %w", err)
		}

		headResp, err := b.client.Do(headReq)
		if err != nil {
			return "", fmt.Errorf("failed to get object metadata: %w", err)
		}
		defer headResp.Body.Close()

		if headResp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("failed to get object metadata: %s", headResp.Status)
		}

		return strings.Trim(headResp.Header.Get("ETag"), "\""), nil
	}

	return "", fmt.Errorf("unsupported bucket type")
}

func (b *Bucket) GetObject(key string) ([]byte, error) {
	if !b.available {
		return nil, fmt.Errorf("bucket is not available")
	}

	if b.Type == BucketTypeS3 || b.Type == BucketTypeMinio {
		input := &s3.GetObjectInput{
			Bucket: aws.String(b.Name),
			Key:    aws.String(key),
		}

		// Use unsigned request
		req, output := b.s3Client.GetObjectRequest(input)
		req.Config.Credentials = credentials.AnonymousCredentials
		req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		err := req.Send()
		if err != nil {
			return nil, fmt.Errorf("failed to get object: %w", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read object data: %w", err)
		}

		return data, nil
	} else if b.Type == BucketTypeHTTP {
		url := fmt.Sprintf("%s/%s/%s", b.Endpoint, b.Name, key)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP request: %w", err)
		}

		resp, err := b.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to get object: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to get object: %s", resp.Status)
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read object data: %w", err)
		}

		return data, nil
	}

	return nil, fmt.Errorf("unsupported bucket type")
}

func (b *Bucket) HeadObject(key string) (string, error) {
	if !b.available {
		return "", fmt.Errorf("bucket is not available")
	}

	if b.Type == BucketTypeS3 || b.Type == BucketTypeMinio {
		input := &s3.HeadObjectInput{
			Bucket: aws.String(b.Name),
			Key:    aws.String(key),
		}

		// Use unsigned request
		req, output := b.s3Client.HeadObjectRequest(input)
		req.Config.Credentials = credentials.AnonymousCredentials
		req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		err := req.Send()
		if err != nil {
			return "", fmt.Errorf("failed to head object: %w", err)
		}

		return strings.Trim(*output.ETag, "\""), nil
	} else if b.Type == BucketTypeHTTP {
		url := fmt.Sprintf("%s/%s/%s", b.Endpoint, b.Name, key)
		req, err := http.NewRequest(http.MethodHead, url, nil)
		if err != nil {
			return "", fmt.Errorf("failed to create HTTP request: %w", err)
		}

		resp, err := b.client.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to head object: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("failed to head object: %s", resp.Status)
		}

		return strings.Trim(resp.Header.Get("ETag"), "\""), nil
	}

	return "", fmt.Errorf("unsupported bucket type")
}

func (b *Bucket) DeleteObject(key string) error {
	if !b.available {
		return fmt.Errorf("bucket is not available")
	}

	if b.Type == BucketTypeS3 || b.Type == BucketTypeMinio {
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(b.Name),
			Key:    aws.String(key),
		}

		// Use unsigned request
		req, _ := b.s3Client.DeleteObjectRequest(input)
		req.Config.Credentials = credentials.AnonymousCredentials
		req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		err := req.Send()
		if err != nil {
			return fmt.Errorf("failed to delete object: %w", err)
		}

		return nil
	} else if b.Type == BucketTypeHTTP {
		url := fmt.Sprintf("%s/%s/%s", b.Endpoint, b.Name, key)
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create HTTP request: %w", err)
		}

		resp, err := b.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to delete object: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("failed to delete object: %s", resp.Status)
		}

		return nil
	}

	return fmt.Errorf("unsupported bucket type")
}

func (b *Bucket) IsAvailable() bool {
	return b.available
}

func (b *Bucket) SetAvailable(available bool) {
	b.available = available
}

func (b *Bucket) GetURL() string {
	return b.URL
}
