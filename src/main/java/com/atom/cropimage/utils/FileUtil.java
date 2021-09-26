package com.atom.cropimage.utils;

import java.io.File;

/**
 * @author Atom
 */
public class FileUtil {

    public static void createParentDir(File file) {
        File parentFile = file.getParentFile();
        if (parentFile != null && !parentFile.exists()) {
            parentFile.mkdirs();
            createParentDir(parentFile);
        }
    }

}
