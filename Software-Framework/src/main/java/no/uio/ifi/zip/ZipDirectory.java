// From https://www.baeldung.com/java-compress-and-uncompress
package no.uio.ifi.zip;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipDirectory {
    public static void main(String[] args) throws IOException {
        String sourceFile = "zipTest";
        FileOutputStream fos = new FileOutputStream("dirCompressed.zip");
        ZipOutputStream zipOut = new ZipOutputStream(fos);
        File fileToZip = new File(sourceFile);

        zipFile(fileToZip, fileToZip.getName(), zipOut);
        zipOut.close();
        fos.close();
    }

    public static void zipDirectory(String sourceFile, String destFileLocation) throws IOException {
        //System.out.println("Zipping directory " + sourceFile + " to " + destFileLocation);
        File destFile = new File(destFileLocation);
        destFile.createNewFile();
        FileOutputStream fos = new FileOutputStream(destFile, false);
        ZipOutputStream zipOut = new ZipOutputStream(fos);
        File fileToZip = new File(sourceFile);

        zipFile(fileToZip, fileToZip.getName(), zipOut);
        zipOut.close();
        fos.close();
    }

    private static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }
        if (fileToZip.isDirectory()) {
            if (fileName.endsWith("/")) {
                zipOut.putNextEntry(new ZipEntry(fileName));
                zipOut.closeEntry();
            } else {
                zipOut.putNextEntry(new ZipEntry(fileName + "/"));
                zipOut.closeEntry();
            }
            File[] children = fileToZip.listFiles();
            //System.out.println("zipping directory");
            for (File childFile : children) {
                //System.out.println("zipping file within directory: " + fileName + "/" + childFile.getName());
                zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
            }
            //System.out.println("Finished zipping directory to " + zipOut);
            return;
        }
        FileInputStream fis = new FileInputStream(fileToZip);
        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOut.putNextEntry(zipEntry);
        byte[] bytes = new byte[1024];
        int length;
        while ((length = fis.read(bytes)) >= 0) {
            zipOut.write(bytes, 0, length);
        }
        fis.close();
    }
}
