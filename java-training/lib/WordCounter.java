package Spark_Scala_tasks.exercise01.Java.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;

public class WordCounter {

    private BufferedReader reader;
    private Integer wordCount = 0;

    public WordCounter(BufferedReader reader) {
        this.reader = reader;
    }

    // method to count words and return a hashmap
    public HashMap CountWords() throws IOException {

        // initializing line
        String line = "";

        HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();

        // traverse through all lines of input
        while ((line = reader.readLine()) != null) {

            // remove unwanted characters
            line = line.replaceAll("[^a-zA-Z0-9]", "");

            // \\s+ is the white-space delimiter in java
            String[] wordList = line.split("\\s+");

            // Iterate through wordList to count unique ords
            for (String item : wordList) {
                if(!wordCounts.containsKey(item)){ //add the word if it isn't added already
                    wordCounts.put(item, 1);
                } else {
                    wordCounts.put(item, wordCounts.get(item) + 1);
                }
            }

            // total count of words
            wordCount += wordList.length;
        }
        return wordCounts;
    }

    // method to count total no of words
    public Integer countTotalWords() {
        return  this.wordCount;
    }
}
