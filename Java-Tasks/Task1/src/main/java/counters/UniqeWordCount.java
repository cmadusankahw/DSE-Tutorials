package counters;

import records.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class UniqeWordCount implements Counter {

    private final Record record;

    public UniqeWordCount(Record record) {
        this.record = record;
    }

    public int getWordCount() {

        ArrayList <String>wordList=new ArrayList();

//        Read Each Lines and add words to an array list
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

//        Check availability of each word and put count and the word. Word =>key Count=>Value
        Map<String, Integer> wordMap = new HashMap<String, Integer>();
        for (String word:wordList) {

            if (!wordMap.containsKey(word)) {
                wordMap.put(word, 1);
            }
            else {
                int count = wordMap.get(word);
                wordMap.put(word, count + 1);
            }
        }
        return wordMap.size();
    }
}
