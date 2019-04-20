package de.semangit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

class OutputSplitter {
    private int maxSize;
    private String rootPath;

    private int fileCtr;
    private int lineCtr;
    private BufferedWriter w;

    OutputSplitter(String path, int maxSize)
    {
        if(Converter.fileSizeBeforeSplit != 0) {
            if (!path.endsWith("/")) {
                path += "/";
            }
            rootPath = path;
            File index = new File(rootPath);
            if (!index.exists() && !index.mkdirs()) {
                System.out.println("Unable to create splitted directory " + rootPath + " . Exiting.");
                System.exit(1);
            }
            this.maxSize = maxSize * 1024 * 1024;
            fileCtr = 0;
            lineCtr = 0;
            try {
                w = new BufferedWriter(new FileWriter(rootPath + fileCtr + ".ttl"));
                PrintPrefixes();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }


    void write(String s)
    {
        try {
            w.write(s);
            lineCtr++;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            //System.exit(1);
        }
        if(lineCtr > 10000)
        {
            lineCtr = 0;
            CheckFileSize();
        }
    }

    private void CheckFileSize()
    {
        try {
            if (Files.size(Paths.get(rootPath + fileCtr + ".ttl")) > maxSize) {
                w.close();
                fileCtr++;
                w = new BufferedWriter(new FileWriter(rootPath + fileCtr + ".ttl"));
                PrintPrefixes();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }

    void CloseWriter()
    {
        if(Converter.fileSizeBeforeSplit != 0) {
            try {
                w.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void PrintPrefixes()
    {
        try {
            final Set<Map.Entry<String, String>> entries = Converter.prefixTable.entrySet();
            w.write("@prefix semangit: <http://semangit.de/ontology/>.");
            for (Map.Entry<String, String> entry : entries) {
                w.write("@prefix " + entry.getValue() + ": <http://semangit.de/ontology/" + entry.getKey() + "#>.");
                w.newLine();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
