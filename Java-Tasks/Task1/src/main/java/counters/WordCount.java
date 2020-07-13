package counters;

import records.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class WordCount implements Counter{

    private final Record record;

    public WordCount(Record record) {
        this.record = record;
    }

    public int getWordCount() {

        ArrayList <String>wordList=new ArrayList();

//        Read each lines of the text File and split words and increase count variable
        try {

            BufferedReader bufferedReader = record.getReader();
            String line = bufferedReader.readLine();

            while (line != null) {
                String[] words = line.split(" ");
                for (String word : words) {
                    word=word.replaceAll("[,()\"\"]","").replaceAll("\\[\\d+\\]","").toLowerCase();
                    wordList.add(word);
                }
                line = bufferedReader.readLine();
            }

        }catch (IOException e){

        }
        return wordList.size();
    }


}
