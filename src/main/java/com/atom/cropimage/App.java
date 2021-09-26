package com.atom.cropimage;

import com.atom.cropimage.action.ScaleImageAction;
import com.atom.cropimage.utils.AwsS3Util;
import com.atom.cropimage.utils.FileUtil;
import com.atom.cropimage.utils.ImageScaleUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * @author Atom
 */
@Slf4j
public class App {

    private static final String sourceBucket = "bucket-liuyaming-001";
    private static final String destBucket = "bucket-liuyaming-002";
    private static final String localFilePath = "/Users/atom/test/";


    public static void main(String[] args) {
//        testSaveToLocalThenUploadS3();
//        testTransferInputStreamThenUploadS3Direct();
        testForkJoinTask();


    }


    /**
     * ---------------------------------------------
     * ns         %     Task name
     * ---------------------------------------------
     * 13080373915  100%  scale image use temp local file task
     */
    private static void testSaveToLocalThenUploadS3() {
        StopWatch sw = new StopWatch();
        sw.start("scale image use temp local file task");
        List<String> objectKeyList = AwsS3Util.listAllObjects(sourceBucket);
        objectKeyList.forEach(keyName -> {
            InputStream objectInputStream = AwsS3Util.getObjectInputStream(sourceBucket, keyName);
            File destImageFile = new File(localFilePath + keyName);
            FileUtil.createParentDir(destImageFile);
            ImageScaleUtil.scaleAndSaveLocal(objectInputStream, destImageFile, keyName, 0.5f);
            AwsS3Util.putLocalObject(destBucket, keyName, localFilePath + keyName);
            try {
                Files.deleteIfExists(Paths.get(localFilePath + keyName));
            } catch (IOException e) {
                log.error("delete the temp file fail ", e);
            }
        });
        //todo delete all empty directory.
//        try (Stream<Path> walk = Files.walk(Paths.get(localFilePath))) {
//            walk.sorted(Comparator.reverseOrder())
//                    .map(Path::toFile)
//                    .peek(System.err::println)
//                    .forEach(File::delete);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        try {
            FileUtils.cleanDirectory(new File(localFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        }


        sw.stop();
        log.debug(sw.prettyPrint());
    }


    /**
     * ---------------------------------------------
     * ns         %     Task name
     * ---------------------------------------------
     * 13983944223  100%  scale image use stream direct task
     */
    private static void testTransferInputStreamThenUploadS3Direct() {
        StopWatch sw = new StopWatch();
        sw.start("scale image use stream direct task");
        List<String> objectKeyList = AwsS3Util.listAllObjects(sourceBucket);
        objectKeyList.forEach(keyName -> {
            InputStream objectInputStream = AwsS3Util.getObjectInputStream(sourceBucket, keyName);
            InputStream inputStream = ImageScaleUtil.scaleToInputStream(objectInputStream, keyName, 0.5f);
            AwsS3Util.putLocalObjectFromInputStream(destBucket, keyName, inputStream);
        });
        sw.stop();
        log.debug(sw.prettyPrint());
    }


    /**
     * ---------------------------------------------
     * ns         %     Task name
     * ---------------------------------------------
     * 5555459959  100%  scale image use stream direct task
     */
    private static void testForkJoinTask() {
        StopWatch sw = new StopWatch();
        sw.start("scale image use stream direct task");
        List<String> objectKeyList = AwsS3Util.listAllObjects(sourceBucket);
        new ForkJoinPool(64).invoke(new ScaleImageAction(sourceBucket, destBucket, objectKeyList));
        sw.stop();
        log.debug(sw.prettyPrint());
    }
}
