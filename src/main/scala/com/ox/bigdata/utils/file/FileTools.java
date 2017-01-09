package com.ox.bigdata.utils.file;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileTools {
    public static boolean getFileNameInFolder(File file, List<String> lstFiles) {
        if (!file.isDirectory()) {
            return false;
        }

        String[] aarFiles = file.list();

        if (aarFiles == null) {
            return false;
        }

        lstFiles.clear();
        for (String filename : aarFiles) {
            lstFiles.add(filename);
        }

        return true;
    }


    public static boolean CreateFolder(File file) {
        if (file.exists()) {
            return true;
        }

        if (!file.mkdir()) {
            return false;
        }

        String strOSName = System.getProperty("os.titles");
        if (!strOSName.toUpperCase().contains("WINDOWS")) {
            try {
                String strCMD = "chmod 777 " + file.getCanonicalPath();
                Runtime.getRuntime().exec(strCMD);
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }


    public static boolean CreateFolderWithParent(File file) {
        if (file.exists()) {
            return true;
        }

        if (!file.mkdirs()) {
            return false;
        }
        return true;
    }


    public static boolean DeleteFile(File file) {
        if (!file.exists()) {
            return true;
        }

        if (!file.isFile()) {
            return false;
        }

        if (!file.delete()) {
            return false;
        }

        return true;
    }

    public static boolean DeleteFolder(File file) {
        if (!file.isDirectory()) {
            return false;
        }

        if (!file.exists()) {
            return true;
        }

        File[] m_arrFiles = file.listFiles();
        for (File subfile : m_arrFiles) {
            if (subfile.isDirectory()) {
                DeleteFolder(subfile);
            } else {
                DeleteFile(subfile);
            }
        }
        return file.delete();
    }


    public static boolean CopyFile(File srcFile, File objFile) {
        boolean srcExist = srcFile.exists();
        boolean objExist = objFile.exists();

        if (!srcExist) {
            return false;
        }

        if (srcFile.equals(objFile)) {
            return true;
        }

        if (objExist) {
            if (!DeleteFile(objFile)) {
                return false;
            }
        }

        try {
            FileInputStream Input = new FileInputStream(srcFile);
            FileOutputStream Output = new FileOutputStream(objFile);
            byte[] Buffer = new byte[1024];
            int Size;
            while ((Size = Input.read(Buffer)) != -1) {
                Output.write(Buffer, 0, Size);
            }
            Input.close();
            Output.flush();
            Output.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 拷贝文件
     */
    public static boolean CopyFile(InputStream src, OutputStream tgt) {
        try {
            if (src == null || tgt == null) {
                return false;
            }

            byte[] Buffer = new byte[1024];
            int Size;
            while ((Size = src.read(Buffer)) != -1) {
                tgt.write(Buffer, 0, Size);
            }
            tgt.flush();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean ZipFolder(File srcFolder, File objZip) {
        if (!srcFolder.isDirectory()) {
            return false;
        }

        if (objZip.exists()) {
            if (!DeleteFile(objZip)) {
                return false;
            }
        }

        try {
            File[] FileList = srcFolder.listFiles();
            ZipOutputStream Zip = new ZipOutputStream(new FileOutputStream(objZip));
            byte[] Buffer = new byte[4096];
            for (int i = 0; i < FileList.length; i++) {
                File f = FileList[i];
                ZipEntry Entry = new ZipEntry(f.getName());
                Entry.setSize(f.length());
                Entry.setTime(f.lastModified());
                Zip.putNextEntry(Entry);
                InputStream instream = new BufferedInputStream(new FileInputStream(f));
                int readLen = 0;
                while ((readLen = instream.read(Buffer, 0, 4096)) != -1) {
                    Zip.write(Buffer, 0, readLen);
                }
                instream.close();
            }
            Zip.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    public static boolean MoveFile(File srcFile, File objFile) {
        boolean srcExist = srcFile.exists();
        boolean objExist = objFile.exists();

        if (!srcExist) {
            return false;
        }

        if (srcFile.equals(objFile)) {
            return true;
        }

        if (objExist) {
            if (!DeleteFile(objFile)) {
                return false;
            }
        }

        if (!srcFile.renameTo(objFile)) {
            return false;
        }

        return true;
    }


    public static boolean Dos2Unix(File folder, String strReg) {
        if (!folder.isDirectory()) {
            return false;
        }

        Pattern pattern = Pattern.compile(strReg);
        File[] arrFiles = folder.listFiles();
        for (File file : arrFiles) {
            if (!file.isFile()) {
                continue;
            }

            Matcher matcher = pattern.matcher(file.getName());
            if (!matcher.matches()) {
                continue;
            }

            if (!Dos2Unix(file)) {
                return false;
            }
        }

        return true;
    }


    public static boolean Dos2Unix(File file) {
        try {
            BufferedWriter bufferedWriter = null;

            BufferedReader bufferedReader = null;

            try {
                if (!file.exists()) {
                    return false;
                }

                if (!file.isFile()) {
                    return false;
                }

                File newFile = File.createTempFile("zte", ".tmp", file.getParentFile());

                bufferedReader = new BufferedReader(new FileReader(file));
                bufferedWriter = new BufferedWriter(new FileWriter(newFile, false));

                String strContent;
                while ((strContent = bufferedReader.readLine()) != null) {
                    bufferedWriter.write(strContent);
                    bufferedWriter.write(",\n");
                }

                bufferedWriter.flush();
                bufferedWriter.close();
                bufferedWriter = null;

                bufferedReader.close();
                bufferedReader = null;

                if (!MoveFile(newFile, file)) {
                    return false;
                }

                return true;

            } finally {
                if (bufferedWriter != null) {
                    bufferedWriter.flush();
                    bufferedWriter.close();
                }

                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            }
        } catch (Exception e) {
            return false;
        }
    }


    public static boolean ReadCSVFile(File file, int skip, List<String[]> data) {
        try {
            CSVReader csvReader = null;

            try {
                if (data == null) {
                    return false;
                }

                if (skip < 0) {
                    return false;
                }

                if (!file.exists()) {
                    return false;
                }

                if (!file.isFile()) {
                    return false;
                }

                csvReader = new CSVReader(new FileReader(file), ',', '\"', skip);

                String[] nextLine;
                while ((nextLine = csvReader.readNext()) != null) {
                    data.add(nextLine);
                }

                return true;
            } finally {
                if (csvReader != null) {
                    csvReader.close();
                }
            }
        } catch (Exception e) {
            return false;
        }
    }


    public static boolean WriteCSVFile(File file, List<String[]> data, boolean append, String lineEnd) {
        try {
            CSVWriter csvWriter = null;

            try {
                if (data == null) {
                    return false;
                }
                String absPath = file.getAbsolutePath();
                new File(absPath.substring(0, absPath.lastIndexOf(File.separator))).mkdirs();
                csvWriter = new CSVWriter(new FileWriter(file, append), ',', '\"', lineEnd);
                csvWriter.writeAll(data);

                return true;
            } finally {
                if (csvWriter != null) {
                    csvWriter.close();
                }
            }
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean getFileByPattern(File baseFolder, final String pattern, boolean excludeFolder, List<File> files) {
        try {
            if (!baseFolder.isDirectory()) {
                return false;
            }

            File[] arrFile = baseFolder.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File arg0, String arg1) {
                    if (arg1.contains(pattern)) {
                        return true;
                    }
                    return false;
                }
            });

            if (arrFile == null) {
                return false;
            }

            for (File file : arrFile) {
                if (excludeFolder && file.isDirectory()) {
                    continue;
                }

                files.add(file);
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

