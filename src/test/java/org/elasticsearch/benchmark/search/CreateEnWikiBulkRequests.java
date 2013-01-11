package org.elasticsearch.benchmark.search;

import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class CreateEnWikiBulkRequests {

    public static final int MAX_DOCS = 5000000;
    public static final int BATCH = 1000;
    public static final String EN_WIKI_FILE = "/Users/mvg/Temp/enwiki-20121201-pages-articles.xml.gz2";
    public static final String DIRECTORY_PATH = "/Users/mvg/development/data-sets/enwiki";
    public static final String FILE_NAME = "requests";
    public static final String FILE_EXTENSION = "json";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("docs.file", EN_WIKI_FILE);
        properties.setProperty("content.source.forever", "false");
        properties.setProperty("keep.image.only.docs", "false");
        Config config = new Config(properties);

        ContentSource contentSource = new EnwikiContentSource();
        contentSource.setConfig(config);

        DocMaker docMaker = new DocMaker();
        docMaker.setConfig(config, contentSource);
        docMaker.resetInputs();

        System.out.println("Starting extracting enwiki");
        StopWatch stopWatch = new StopWatch().start();

        int counter = 0;
        OutputStream out = newOut(counter);
        try {
            for (Document doc = docMaker.makeDocument(); doc != null; doc = docMaker.makeDocument()) {
                BytesReference header = jsonBuilder()
                        .startObject()
                        .startObject("index")
                        .field("_id", doc.get(DocMaker.ID_FIELD))
                        .endObject()
                        .endObject()
                        .bytes();
                out.write(header.array(), header.arrayOffset(), header.length());
                out.write('\n');

                BytesReference source = jsonBuilder()
                        .startObject()
                        .field("name", doc.get(DocMaker.NAME_FIELD))
                        .field("title", doc.get(DocMaker.TITLE_FIELD))
                        .field("date", doc.get(DocMaker.DATE_FIELD))
                        .field("body", doc.get(DocMaker.BODY_FIELD))
                        .endObject()
                        .bytes();
                out.write(source.array(), source.arrayOffset(), source.length());
                out.write('\n');

                if (++counter >= MAX_DOCS) {
                    break;
                }

                if (counter % BATCH == 0) {
                    out.flush();
                    out.close();
                    out = newOut(counter);
                }
            }
        } catch (NoMoreDataException e) {
            //don't fail
        } finally {
            docMaker.close();
            out.flush();
            out.close();
        }
    }

    private static OutputStream newOut(int counter) throws FileNotFoundException {
        String start = String.format("%010d", counter);
        String end = String.format("%d", (counter + BATCH - 1));
        return new BufferedOutputStream(new FileOutputStream(DIRECTORY_PATH + '/' + FILE_NAME + "[" + start + '-' + end + "]." + FILE_EXTENSION));
    }


}
