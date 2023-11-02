package main

import (
	"context"
	// "crypto/md5"
	// "flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	// "path"
	// "strconv"
	"flag"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"slices"
	"sort"
	"strings"
	"time"
)

type ObjVerInfo struct {
	S3Client     *minio.Client
	VersionID    string
	LastModified time.Time
	ETag         string
}

// type objVersionList []ObjVerInfo

// func (p objVersionList) Len() int {
// 	return len(p)
// }

// func (p objVersionList) Less(i, j int) bool {
// 	return p[i].LastModified.Before(p[j].LastModified)
// }

// func (p objVersionList) Swap(i, j int) {
// 	p[i], p[j] = p[j], p[i]
// }

var (
	s1Endpoint, s1AccessKey, s1SecretKey string
	s2Endpoint, s2AccessKey, s2SecretKey string
	bucket, object                       string
	versionCount                         int
	insecure                             bool
)

func main() {
	flag.StringVar(&s1Endpoint, "s1-endpoint", "", "Site1 S3 endpoint URL")
	flag.StringVar(&s1AccessKey, "s1-access-key", "", "Site1 S3 Access Key")
	flag.StringVar(&s1SecretKey, "s1-secret-key", "", "Site1 S3 Secret Key")
	flag.StringVar(&s2Endpoint, "s2-endpoint", "", "Site2 S3 endpoint URL")
	flag.StringVar(&s2AccessKey, "s2-access-key", "", "Site2 S3 Access Key")
	flag.StringVar(&s2SecretKey, "s2-secret-key", "", "Site2 S3 Secret Key")
	flag.StringVar(&bucket, "bucket", "", "Select a specific bucket")
	flag.StringVar(&object, "object", "", "Select an object")
	flag.IntVar(&versionCount, "versions", 0, "No.of versions to retain")
	flag.BoolVar(&insecure, "insecure", false, "Disable TLS verification")
	flag.Parse()

	if s1Endpoint == "" {
		log.Fatalln("Site1 Endpoint is not provided")
	}

	if s1AccessKey == "" {
		log.Fatalln("Site1 Access key is not provided")
	}

	if s1SecretKey == "" {
		log.Fatalln("Site1 Secret key is not provided")
	}

	if s2Endpoint == "" {
		log.Fatalln("Site2 Endpoint is not provided")
	}

	if s2AccessKey == "" {
		log.Fatalln("Site2 Access key is not provided")
	}

	if s2SecretKey == "" {
		log.Fatalln("Site2 Secret key is not provided")
	}

	if bucket == "" {
		log.Fatalln("Bucket is not provided")
	}

	if object == "" {
		log.Fatalln("Object is not provided")
	}

	if versionCount == 0 {
		log.Fatalln("Version count is not provided")
	}

	// s3Client, err := minio.New("localhost:9091", &minio.Options{
	// 	Creds:  credentials.NewStaticV4("minio", "minio123", ""),
	// 	Secure: false,
	// })

	s1S3Client := getS3Client(s1Endpoint, s1AccessKey, s1SecretKey, insecure)
	s2S3Client := getS3Client(s2Endpoint, s2AccessKey, s2SecretKey, insecure)

	opts := minio.ListObjectsOptions{
		Recursive:    true,
		Prefix:       object,
		WithVersions: true,
		WithMetadata: true,
	}

	allVersions := []ObjVerInfo{}
	count := 0
	for obj := range s1S3Client.ListObjects(context.Background(), bucket, opts) {
		if count >= versionCount {
			break
		}
		if obj.Err != nil {
			log.Println("FAILED: LIST with error:", obj.Err)
			continue
		}
		if obj.IsDeleteMarker {
			log.Println("SKIPPED: DELETE marker object:", object)
			continue
		}
		allVersions = append(allVersions, ObjVerInfo{S3Client: s1S3Client, VersionID: obj.VersionID, LastModified: obj.LastModified, ETag: obj.ETag})
		count = count + 1
	}
	fmt.Printf("Got %d versions from Site1\n", len(allVersions))

	count = 0
	for obj := range s2S3Client.ListObjects(context.Background(), bucket, opts) {
		if count >= versionCount {
			break
		}
		if obj.Err != nil {
			log.Println("FAILED: LIST with error:", obj.Err)
			return
		}
		if obj.IsDeleteMarker {
			log.Println("SKIPPED: DELETE marker object:", object)
			continue
		}
		present := false
		for _, v := range allVersions {
			if v.VersionID == obj.VersionID {
				present = true
				break
			}
		}
		if !present {
			allVersions = append(allVersions, ObjVerInfo{S3Client: s2S3Client, VersionID: obj.VersionID, LastModified: obj.LastModified, ETag: obj.ETag})
		}
		count = count + 1
	}

	if len(allVersions) == 0 {
		fmt.Println("No versions found")
		return
	}
	sort.Slice(allVersions, func(i, j int) bool {
		return allVersions[i].LastModified.After(allVersions[j].LastModified)
	})

	versions := []ObjVerInfo{}

	for i, v := range allVersions {
		if i == versionCount {
			break
		}
		versions = append(versions, v)
	}
	slices.Reverse(versions)
	fmt.Printf("Found %d unique versions to retain\n", len(versions))

	id := uuid.New()
	for i, v := range versions {
		// download latest N versions
		reader, err := v.S3Client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{VersionID: v.VersionID})
		if err != nil {
			log.Fatalln(err)
		}
		defer reader.Close()

		fileName := fmt.Sprint(id, "-", "v", i+1)
		localFile, err := os.Create(fileName)
		if err != nil {
			log.Fatalln(err)
		}
		defer localFile.Close()

		stat, err := reader.Stat()
		if err != nil {
			log.Fatalln(err)
		}

		if _, err := io.CopyN(localFile, reader, stat.Size); err != nil {
			log.Fatalln(err)
		}
	}
	fileName := fmt.Sprint(id, "-out")
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer file.Close()
	_, err = file.WriteString(fmt.Sprint(bucket, "/", object))
	if err != nil {
		fmt.Println("Failed to write to file:", err) //print the failed message
		return
	}
	fmt.Printf("Downloaded %d versions\n", len(versions))

	removeOpts := minio.RemoveObjectOptions{
		ForceDelete: true,
	}

	err = s1S3Client.RemoveObject(context.Background(), bucket, object, removeOpts)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Purged object %s on Site1\n", object)
	err = s2S3Client.RemoveObject(context.Background(), bucket, object, removeOpts)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Purged object %s on Site2\n", object)

	for i, v := range versions {
		fileName := fmt.Sprint(id, "-", "v", i+1)
		obj, err := os.Open(fileName)
		if err != nil {
			log.Fatalln(err)
		}
		defer obj.Close()
		objectStat, err := obj.Stat()
		if err != nil {
			log.Fatalln(err)
		}

		_, err = s1S3Client.PutObject(context.Background(), bucket, object, obj, objectStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("Uploaded", object, "with ETag", v.ETag, "to Site1")
	}
}

func getS3Client(endpoint string, accessKey string, secretKey string, insecure bool) *minio.Client {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatalln(err)
	}

	secure := strings.EqualFold(u.Scheme, "https")
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		log.Fatalln(err)
	}
	if insecure {
		// skip TLS verification
		transport.TLSClientConfig.InsecureSkipVerify = true
	}

	s3Client, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    secure,
		Transport: transport,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return s3Client
}
