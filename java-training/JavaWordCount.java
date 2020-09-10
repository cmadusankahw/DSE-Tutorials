package Spark_Scala_tasks.exercise01.Java;

import Spark_Scala_tasks.exercise01.Java.lib.CharCounter;
import Spark_Scala_tasks.exercise01.Java.lib.FileCheck;
import Spark_Scala_tasks.exercise01.Java.lib.SentenceCounter;
import Spark_Scala_tasks.exercise01.Java.lib.WordCounter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class JavaWordCount {
    public static void main(String[] args) throws IOException {
        // create a new file object with importing the text file
        File file = new File("in/word_count.text");

        // check file input and get a Reader
        FileCheck fileChecked = new FileCheck(file);
        BufferedReader reader = fileChecked.readFile();

        // Initializing counters
        WordCounter wordCounter = new WordCounter(reader);
        CharCounter characterCounter = new CharCounter(reader);
        SentenceCounter sentenceCounter = new SentenceCounter(reader);

        // get word counts in a hashmap
        HashMap wordCounts = wordCounter.CountWords();

        // get total word count
        Integer wordCount = wordCounter.countTotalWords();

        // get total character count
        Integer characterCount = characterCounter.CountChar();

        // get total sentence count
        Integer sentenceCount = sentenceCounter.CountSentences();

        // print counts
        System.out.println("Total no of Words = " + wordCount);
        System.out.println("Total no of sentences = " + sentenceCount);
        System.out.println("Total no of characters = " + characterCount +'\n');
        System.out.println("Word Counts : \n" + wordCounts);
    }
}

