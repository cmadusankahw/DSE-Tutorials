import counters.Counter;
import counters.UniqeWordCount;
import counters.WordCount;
import org.junit.Test;
import records.Record;
import records.TextFileRecord;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UniqeWordCountTest {

    @Test
    public void shouldReturnUniqeWordCount_when_GiveAValidFile()  {

        Record textRecord = new TextFileRecord("D:\\Zone24x7\\Java_Task1\\src\\main\\resources\\Java Task 01.txt");
        Counter wordCount = new UniqeWordCount(textRecord);
        assertThat(wordCount.getWordCount(),is(107));


    }
}
