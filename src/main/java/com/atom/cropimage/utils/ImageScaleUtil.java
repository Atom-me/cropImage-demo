package com.atom.cropimage.utils;


import lombok.extern.slf4j.Slf4j;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Arrays;
import java.util.Iterator;


/**
 * @author Atom
 */
@Slf4j
public class ImageScaleUtil {

    /**
     * get image format
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static String getImageFormatName(File file) throws IOException {
        String formatName = null;
        ImageInputStream iis = ImageIO.createImageInputStream(file);
        Iterator<ImageReader> imageReader = ImageIO.getImageReaders(iis);
        if (imageReader.hasNext()) {
            ImageReader reader = imageReader.next();
            formatName = reader.getFormatName();
        }
        return formatName;
    }


    /**
     * read image file to BufferedImage.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static BufferedImage[] readerImage(File file) throws IOException {
        BufferedImage sourceImage = ImageIO.read(file);
        BufferedImage[] images = null;
        ImageInputStream iis = ImageIO.createImageInputStream(file);
        Iterator<ImageReader> imageReaders = ImageIO.getImageReaders(iis);
        if (imageReaders.hasNext()) {
            ImageReader reader = imageReaders.next();
            reader.setInput(iis);
            int imageNumber = reader.getNumImages(true);
            images = new BufferedImage[imageNumber];
            for (int i = 0; i < imageNumber; i++) {
                BufferedImage image = reader.read(i);
                if (sourceImage.getWidth() > image.getWidth() || sourceImage.getHeight() > image.getHeight()) {
                    image = zoom(image, sourceImage.getWidth(), sourceImage.getHeight());
                }
                images[i] = image;
            }
            reader.dispose();
            iis.close();
        }
        return images;
    }

    /**
     * cut image use specific size
     *
     * @param images
     * @param x
     * @param y
     * @param width
     * @param height
     * @return
     */
    public static BufferedImage[] processImage(BufferedImage[] images, int x, int y, int width, int height) {
        if (null == images) {
            return null;
        }
        BufferedImage[] oldImages = images;
        images = new BufferedImage[images.length];
        for (int i = 0; i < oldImages.length; i++) {
            BufferedImage image = oldImages[i];
            images[i] = image.getSubimage(x, y, width, height);
        }
        return images;
    }

    /**
     * write post image to local file.
     *
     * @param images
     * @param formatName
     * @param file
     * @throws Exception
     */
    public static void writerImage(BufferedImage[] images, String formatName, File file) throws Exception {
        Iterator<ImageWriter> imageWriters = ImageIO.getImageWritersByFormatName(formatName);
        if (imageWriters.hasNext()) {
            ImageWriter writer = imageWriters.next();
            String fileName = file.getName();
            int index = fileName.lastIndexOf(".");
            if (index > 0) {
                fileName = fileName.substring(0, index + 1) + formatName;
            }
            String pathPrefix = getFilePrefixPath(file.getPath());
            File outFile = new File(pathPrefix + fileName);
            ImageOutputStream ios = ImageIO.createImageOutputStream(outFile);
            writer.setOutput(ios);

            if (writer.canWriteSequence()) {
                writer.prepareWriteSequence(null);
                for (BufferedImage childImage : images) {
                    IIOImage image = new IIOImage(childImage, null, null);
                    writer.writeToSequence(image, null);
                }
                writer.endWriteSequence();
            } else {
                for (BufferedImage image : images) {
                    writer.write(image);
                }
            }

            writer.dispose();
            ios.close();
        }
    }

    /**
     * cut image.
     *
     * @param sourceFile
     * @param destFile
     * @param x
     * @param y
     * @param width
     * @param height
     * @throws Exception
     */
    public static void cutImage(File sourceFile, File destFile, int x, int y, int width, int height) throws Exception {
        BufferedImage[] images = readerImage(sourceFile);
        images = processImage(images, x, y, width, height);
        String formatName = getImageFormatName(sourceFile);
        destFile = new File(getPathWithoutSuffix(destFile.getPath()) + formatName);
        writerImage(images, formatName, destFile);
    }


    /**
     * get os support image format.
     */
    public static void getOSSupportsStandardImageFormat() {
        String[] readerFormatName = ImageIO.getReaderFormatNames();
        String[] readerSuffixName = ImageIO.getReaderFileSuffixes();
        String[] readerMIMEType = ImageIO.getReaderMIMETypes();
        log.info("========================= OS supports reader ========================");
        log.info("OS supports reader format name :  " + Arrays.asList(readerFormatName));
        log.info("OS supports reader suffix name :  " + Arrays.asList(readerSuffixName));
        log.info("OS supports reader MIME type :  " + Arrays.asList(readerMIMEType));

        String[] writerFormatName = ImageIO.getWriterFormatNames();
        String[] writerSuffixName = ImageIO.getWriterFileSuffixes();
        String[] writerMIMEType = ImageIO.getWriterMIMETypes();

        log.info("========================= OS supports writer ========================");
        log.info("OS supports writer format name :  " + Arrays.asList(writerFormatName));
        log.info("OS supports writer suffix name :  " + Arrays.asList(writerSuffixName));
        log.info("OS supports writer MIME type :  " + Arrays.asList(writerMIMEType));
    }

    /**
     * zoom image.
     *
     * @param sourceImage
     * @param width
     * @param height
     * @return
     */
    private static BufferedImage zoom(BufferedImage sourceImage, int width, int height) {
        BufferedImage zoomImage = new BufferedImage(width, height, sourceImage.getType());
        Image image = sourceImage.getScaledInstance(width, height, Image.SCALE_SMOOTH);
        Graphics gc = zoomImage.getGraphics();
        gc.setColor(Color.WHITE);
        gc.drawImage(image, 0, 0, null);
        return zoomImage;
    }

    /**
     * get file prefix path. not contains current file.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static String getFilePrefixPath(File file) throws IOException {
        if (!file.exists()) {
            throw new IOException("not found the file !");
        }
        String fileName = file.getName();
        return file.getPath().replace(fileName, "");
    }

    /**
     * get file prefix path. not contains current file.
     *
     * @param path
     * @return
     * @throws Exception
     */
    public static String getFilePrefixPath(String path) throws Exception {
        if (null == path || path.isEmpty()) {
            throw new RuntimeException("the gaven path is null！");
        }
        int index = path.lastIndexOf(File.separator);
        if (index > 0) {
            path = path.substring(0, index + 1);
        }
        return path;
    }

    /**
     * get file path without suffix.
     *
     * @param src
     * @return
     */
    public static String getPathWithoutSuffix(String src) {
        String path = src;
        int index = path.lastIndexOf(".");
        if (index > 0) {
            path = path.substring(0, index + 1);
        }
        return path;
    }

    /**
     * get file name,contains suffix.
     *
     * @param filePath
     * @return
     * @throws IOException
     */
    public static String getFileName(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("not found the file !");
        }
        return file.getName();
    }

    /**
     * scale image and transfer to inputStream.
     *
     * @param srcImageFile
     * @param destImageFile
     * @param scale
     * @return
     */
    public static boolean scaleAndSaveLocal(File srcImageFile, File destImageFile, float scale) {
        try {
            BufferedImage read = ImageIO.read(srcImageFile);
            int width = (int) (read.getWidth() * scale);
            int height = (int) (read.getHeight() * scale);
            Image img = read.getScaledInstance(width, height, Image.SCALE_DEFAULT);
            BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            Graphics2D graphics = image.createGraphics();
            graphics.drawImage(img, 0, 0, null);
            graphics.dispose();
            String fileName = srcImageFile.getName();
            String formatName = fileName.substring(fileName.lastIndexOf(".") + 1);
            ImageIO.write(image, formatName, destImageFile);
        } catch (IOException e) {
            log.error("scale image fail ", e);
            return false;
        }
        return true;
    }

    /**
     * scale image and save to local file.
     *
     * @param srcImageFileInputStream
     * @param destImageFile
     * @param fileName
     * @param scale
     * @return
     */
    public static boolean scaleAndSaveLocal(InputStream srcImageFileInputStream, File destImageFile, String fileName, float scale) {
        try {
            BufferedImage bufferedImageRead = ImageIO.read(srcImageFileInputStream);
            int width = (int) (bufferedImageRead.getWidth() * scale);
            int height = (int) (bufferedImageRead.getHeight() * scale);
            Image img = bufferedImageRead.getScaledInstance(width, height, Image.SCALE_DEFAULT);
            BufferedImage bufferedImageWrite = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            Graphics2D graphics = bufferedImageWrite.createGraphics();
            graphics.drawImage(img, 0, 0, null);
            graphics.dispose();
            String formatName = fileName.substring(fileName.lastIndexOf(".") + 1);
            ImageIO.write(bufferedImageWrite, formatName, destImageFile);
        } catch (IOException e) {
            log.error("scale image file fail ", e);
            return false;
        }
        return true;
    }

    /**
     * scale image and transfer to inputStream.
     *
     * @param srcImageFileInputStream
     * @param fileName
     * @param scale
     * @return
     */
    public static InputStream scaleToInputStream(InputStream srcImageFileInputStream, String fileName, float scale) {
        try {
            BufferedImage bufferedImageRead = ImageIO.read(srcImageFileInputStream);
            int width = (int) (bufferedImageRead.getWidth() * scale);
            int height = (int) (bufferedImageRead.getHeight() * scale);
            Image img = bufferedImageRead.getScaledInstance(width, height, Image.SCALE_DEFAULT);
            BufferedImage bufferedImageWrite = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            Graphics2D graphics = bufferedImageWrite.createGraphics();
            graphics.drawImage(img, 0, 0, null);
            graphics.dispose();
            String formatName = fileName.substring(fileName.lastIndexOf(".") + 1);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ImageIO.write(bufferedImageWrite, formatName, bos);
            return new ByteArrayInputStream(bos.toByteArray(), 0, bos.size());
        } catch (IOException e) {
            log.error("scale image file fail ", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            File file = new File("/Users/atom/wallpapers/ubuntu-wallpapers/stylish-ubuntu.jpg");
            String formatName = getImageFormatName(file);
            String pathSuffix = "." + formatName;
            String pathPrefix = getFilePrefixPath(file);
            String originFileName = file.getName().split("\\.")[0];

            String targetPath = pathPrefix + originFileName + "_" + System.currentTimeMillis() + pathSuffix;
            File destFile = new File(targetPath);
            scaleAndSaveLocal(file, destFile, 0.5f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

