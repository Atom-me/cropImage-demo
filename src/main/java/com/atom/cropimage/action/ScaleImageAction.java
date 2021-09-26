package com.atom.cropimage.action;

import com.atom.cropimage.utils.AwsS3Util;
import com.atom.cropimage.utils.ImageScaleUtil;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * @author Atom
 */
@Slf4j
public class ScaleImageAction extends RecursiveAction {

    private String sourceBucket;
    private String destBucket;
    private List<String> objectKeyList;


    public ScaleImageAction(String sourceBucket, String destBucket, List<String> objectKeyList) {
        this.sourceBucket = sourceBucket;
        this.destBucket = destBucket;
        this.objectKeyList = objectKeyList;
    }

    /**
     * The main computation performed by this task.
     */
    @Override
    protected void compute() {
        int size = objectKeyList.size();
        List<ScaleImageAction> subTasks = new LinkedList<>();
        if (size <= 2) {
            scaleImages(objectKeyList);
        } else {
            List<List<String>> objectListPartitions = Lists.partition(objectKeyList, 2);
            objectListPartitions.forEach(part -> subTasks.add(new ScaleImageAction(sourceBucket, destBucket, part)));
            invokeAll(subTasks);
        }
    }


    /**
     * business method
     *
     * @param objectKeyList
     */
    private void scaleImages(List<String> objectKeyList) {
        objectKeyList.forEach(keyName -> {
            InputStream objectInputStream = AwsS3Util.getObjectInputStream(this.sourceBucket, keyName);
            InputStream inputStream = ImageScaleUtil.scaleToInputStream(objectInputStream, keyName, 0.5f);
            AwsS3Util.putLocalObjectFromInputStream(this.destBucket, keyName, inputStream);
        });
    }
}
