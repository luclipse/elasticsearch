package org.elasticsearch.benchmark.search;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.XNIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

/**
 */
public class GenerateMisspelledTerms {

    public static void main(String[] args) throws Exception {
        class TermFreq {

            final BytesRef term;
            final int freq;

            TermFreq(BytesRef term, int freq) {
                this.term = term;
                this.freq = freq;
            }
        }

        int NUM_TERMS = 50000;

        AtomicReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(XNIOFSDirectory.open(new File("/Users/mvg/development/projects/os/my-elasticsearch/data/elasticsearch/nodes/0/indices/enwiki/0/index"))));
        TermsEnum termsEnum = reader.terms("body").iterator(null);
        PriorityQueue<TermFreq> queue = new PriorityQueue<TermFreq>(NUM_TERMS) {
            @Override
            protected boolean lessThan(TermFreq a, TermFreq b) {
                return (a.freq - b.freq) < 0;
            }
        };
        int counter = 0;
        for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
            counter++;
            if (term.length < 6) {
                continue;
            }

            if (queue.size() < NUM_TERMS || queue.top().freq < termsEnum.docFreq()) {
                queue.insertWithOverflow(new TermFreq(BytesRef.deepCopyOf(term), termsEnum.docFreq()));
            }
        }
        System.out.println("Unique terms " + counter);
        PrintStream out = new PrintStream(new FileOutputStream("./50k-misspelled-terms.txt"));
        for (TermFreq termFreq = queue.pop(); termFreq != null; termFreq = queue.pop()) {
            StringBuilder builder = new StringBuilder(termFreq.term.utf8ToString());
            builder.deleteCharAt(builder.length() - 2);
            out.println(builder.toString());
        }
        out.flush();
        out.close();
    }

}
