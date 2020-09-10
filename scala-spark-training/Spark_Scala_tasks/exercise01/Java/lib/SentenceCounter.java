package Spark_Scala_tasks.exercise01.Java.lib;

import java.io.BufferedReader;
import java.io.IOException;

public class SentenceCounter {
    private BufferedReader reader;

    public SentenceCounter(BufferedReader reader) {
        this.reader = reader;
    }

    // method to count total no of sentences
    public Integer CountSentences() throws IOException {

        // initializing line
        String line = "";
        Integer sentenceCount = 0;

        // traverse through all lines of input
        while ((line = reader.readLine()) != null) {

            // remove unwanted characters
            line = line.replaceAll("[^a-zA-Z0-9]", "");

            // [!?.:]+ is the sentence delimiter in java
            String[] sentenceList = line.split("[!?.:]+");
            sentenceCount += sentenceList.length;

        }
        return sentenceCount;
    }
}
