package com.atom.cropimage.utils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import javax.imageio.ImageIO;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Atom
 */
@Slf4j
public class AwsS3Util {


    private static final String ak = "xxx";
    private static final String sk = "xxx";
    private static final S3Client s3Client;
    private static final AwsCredentials awsCredentials;

    static {
        awsCredentials = AwsBasicCredentials.create(ak, sk);
        Region region = Region.CN_NORTHWEST_1;
        s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .region(region)
                .build();
    }


    public static void createBucket(String bucketName) {
        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(response -> log.info("waiter response:[{}]", response));
            log.info("bucket [{}] is ready", bucketName);
        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }


    public static void deleteEmptyBucket(String bucketName) {
        try {
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            log.info("bucket [{}] is deleted successfully", bucketName);
        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }


    public static void deleteAllObjects(String bucketName) {
        ArrayList<ObjectIdentifier> keys = new ArrayList<>();
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3Client.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (S3Object object : objects) {
                log.info("\n The name of the key is [{}]", object.key());
                keys.add(ObjectIdentifier.builder().key(object.key()).build());
            }

            // Delete multiple objects in one request.
            Delete del = Delete.builder()
                    .objects(keys)
                    .build();

            DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(del)
                    .build();
            s3Client.deleteObjects(multiObjectDeleteRequest);
            log.info("Multiple objects are deleted!");
        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }


    public static List<Bucket> listBuckets() {
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listBucketsRequest);
        listBucketsResponse.buckets().forEach(bucket -> log.info(bucket.name()));
        return listBucketsResponse.buckets();
    }


    public static void putLocalObject(String bucketName, String objectKey, String localFilePath) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("atom-metadata-of-object", "test");
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            PutObjectResponse response = s3Client.putObject(putOb, RequestBody.fromBytes(getObjectFile(localFilePath)));
            log.info("put object response is [{}]", response);
        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
        }
    }

    public static void putLocalObjectFromInputStream(String bucketName, String objectKey, InputStream fileInputStream) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("atom-metadata-of-object", "test");
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            // todo use fileInputStream.available() to get the inputStream contentLength maybe wrong, see the javaDoc of available().
            PutObjectResponse response = s3Client.putObject(putOb, RequestBody.fromInputStream(fileInputStream, fileInputStream.available()));
            log.info("put object response is [{}]", response);
        } catch (S3Exception | IOException e) {
            log.error(e.getMessage());
        }
    }


    /**
     * Return a byte array
     */
    private static byte[] getObjectFile(String filePath) {
        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;
        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }


    public static boolean uploadDirectoryOrFile(final String bucketName, final File source) throws FileNotFoundException {
        log.info("uploadDirectoryOrFile invoked, bucketName: {} , Source: {}", bucketName, source.getAbsolutePath());
        if (source.isFile()) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("atom-metadata-of-object", "test");
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(source.getName())
                    .metadata(metadata)
                    .build();
            PutObjectResponse response = s3Client.putObject(putOb, RequestBody.fromBytes(getObjectFile(source.getAbsolutePath())));
            log.info("put object response is [{}]", response);
        } else if (source.isDirectory()) {
            //upload recursively
            uploadDirectory(bucketName, source, true);
        } else {
            throw new FileNotFoundException("File is neither a regular file nor a directory " + source);
        }
        return true;
    }

    private static void uploadDirectory(String bucketName, File source, boolean includeSubdirectories) {
        if (source == null || !source.exists() || !source.isDirectory()) {
            throw new IllegalArgumentException("Must provide a directory to upload");
        }
        List<File> files = new LinkedList<>();
        listFiles(source, files, includeSubdirectories);
        uploadFileList(bucketName, source, files, includeSubdirectories);
    }

    private static void uploadFileList(String bucketName, File source, List<File> files, boolean includeSubdirectories) {
        /*
         * If the absolute path for the common/base directory does NOT end
         * in a separator (which is the case for anything but root
         * directories), then we know there's still a separator between the
         * base directory and the rest of the file's path, so we increment
         * the starting position by one.
         */
        int startingPosition = source.getAbsolutePath().length();
        if (!(source.getAbsolutePath().endsWith(File.separator))) {
            startingPosition++;
        }
        for (File f : files) {
            // Check, if is file, since only files can be uploaded.
            if (f.isFile()) {
                String key = f.getAbsolutePath()
                        .substring(startingPosition)
                        .replaceAll("\\\\", "/");

                Map<String, String> metadata = new HashMap<>();
                metadata.put("atom-metadata-of-object", "test");
                PutObjectRequest putOb = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .metadata(metadata)
                        .build();
                PutObjectResponse response = s3Client.putObject(putOb, RequestBody.fromBytes(getObjectFile(f.getAbsolutePath())));
                log.info("put object response is [{}]", response);
            }
        }
    }

    /**
     * Lists files in the directory given and adds them to the result list
     * passed in, optionally adding subdirectories recursively.
     */
    private static void listFiles(File dir, List<File> results, boolean includeSubDirectories) {
        File[] found = dir.listFiles();
        if (found != null) {
            for (File f : found) {
                if (f.isDirectory()) {
                    if (includeSubDirectories) {
                        listFiles(f, results, includeSubDirectories);
                    }
                } else {
                    results.add(f);
                }
            }
        }
    }


    public static byte[] getObjectBytes(String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            log.info("Successfully obtained bytes from an S3 object");
            return data;
        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
            throw new RuntimeException("get object bytes error ", e);
        }
    }

    public static InputStream getObjectInputStream(String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
            log.info("Successfully obtained bytes from an S3 object");
            return objectBytes.asInputStream();
        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
            throw new RuntimeException("get object inputStream error ", e);
        }
    }

    public static void downloadObjectToLocalFile(String bucketName, String keyName, String localFilePath) {
        byte[] objectBytes = getObjectBytes(bucketName, keyName);
        try {
            // Write the data to a local file
            File myFile = new File(localFilePath);
            OutputStream os = new FileOutputStream(myFile);
            os.write(objectBytes);
            os.close();
        } catch (IOException e) {
            log.error("write object data to local file error ", e);
        }
    }


    public static List<String> listAllObjects(String bucketName) {
        List<String> objectKeyList = new ArrayList<>();
        ListObjectsV2Request listObjectsReqManual = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(1)
                .build();
        boolean done = false;
        while (!done) {
            ListObjectsV2Response listObjResponse = s3Client.listObjectsV2(listObjectsReqManual);
            for (S3Object content : listObjResponse.contents()) {
                String objectKey = content.key();
                log.info(objectKey);
                List<String> supportFieType = Arrays.stream(ImageIO.getReaderFileSuffixes()).collect(Collectors.toList());
                if (objectKey.contains(".") && supportFieType.contains(objectKey.substring(objectKey.lastIndexOf(".") + 1))) {
                    objectKeyList.add(objectKey);
                }
            }
            if (listObjResponse.nextContinuationToken() == null) {
                done = true;
            }
            listObjectsReqManual = listObjectsReqManual.toBuilder()
                    .continuationToken(listObjResponse.nextContinuationToken())
                    .build();
        }
        return objectKeyList;
    }

    public static void main(String[] args) throws FileNotFoundException {
//        createBucket("bucket-liuyaming-002");
//        deleteEmptyBucket("bucket-liuyaming-001");
        deleteAllObjects("bucket-liuyaming-002");
//        listBuckets();
//        putLocalObject("bucket-liuyaming-001", "a.jpg", "/Users/atom/wallpapers/ubuntu-wallpapers/ubuntu-os-orange-animal.jpg");

        uploadDirectoryOrFile("bucket-liuyaming-001", new File("/Users/atom/wallpapers"));

//        downloadObjectToLocalFile("bucket-liuyaming-001","ubuntu-wallpapers/1.png","/Users/atom/test/download-1.png");


    }

}
