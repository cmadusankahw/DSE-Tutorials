package Spark_Scala_tasks.exercise01.Java.lib;

import java.io.*;

public class FileCheck {

    private File file;

    public FileCheck(File file){
        this.file = file;
    }

    // method to read file and return a new BufferReader
    public BufferedReader readFile() {
        // initializing a BufferReader
        BufferedReader reader = null;
        try {

            FileInputStream fileStream = new FileInputStream(file);
            InputStreamReader input = new InputStreamReader(fileStream);
            reader = new BufferedReader(input);

        }catch (FileNotFoundException e){
            // if file not found
            System.out.println("File not Found!");

        }
        return reader;
    }
}
