package Spark_Scala_tasks.exercise01.Java.lib;

import java.io.BufferedReader;
import java.io.IOException;

public class CharCounter {
    private BufferedReader reader;

    public CharCounter(BufferedReader reader) {
        this.reader = reader;
    }

    // method to count total characters
    public Integer CountChar() throws IOException {

        // initializing line
        String line = "";
        Integer characterCount = 0;

        // traverse through all lines of input
        while ((line = reader.readLine()) != null) {

            // remove unwanted characters and whitespaces
            line = line.replaceAll("[^a-zA-Z0-9]", "").replaceAll("\\s+", "");
            characterCount += line.length();

        }
        return characterCount;
    }
}
