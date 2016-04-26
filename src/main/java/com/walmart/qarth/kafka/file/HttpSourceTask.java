package com.walmart.qarth.kafka.file;

import org.apache.http.HttpResponse;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.net.MalformedURLException;
import java.net.URL;
/**
 * Created by vgodbol on 4/21/16.
 */
public class HttpSourceTask extends SourceTask{
    private static final Logger log = LoggerFactory.getLogger(HttpSourceTask.class);
    private static final String FILENAME_FIELD = "filename";
    private static final String POSITIONAL_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final Schema VALUE1_SCHEMA = Schema.OPTIONAL_STRING_SCHEMA;

    private URL url;
    private String filename;
    private InputStream stream;
    private BufferedReader reader = null;

    private int offset = 0;
    private String topic = null;
    private Long streamOffset;
    private char[] buffer = new char[1024];

    @Override
    public String version() {
        return new HttpSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            url = new URL(props.get(HttpSourceConnector.HTTP_URL));
        } catch (MalformedURLException e) {
            throw new ConnectException("HttpSourceConnector: BAD URL ", e);
        }
/*        if (filename == null || filename.isEmpty()) {
            stream = System.in;
            streamOffset = null;
            reader = new BufferedReader(new InputStreamReader(stream));
        }*/
        topic = props.get(HttpSourceConnector.TOPIC_CONFIG);
        if (topic == null) {
            throw new ConnectException("Missing topic");
        }
    }

    //@Override
    public List<SourceRecord> poll_old() throws InterruptedException {
        if (stream == null) {
            try {
                stream = new FileInputStream(filename);
                Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
                if (offset != null) {
                    Object lastRecordOffset = offset.get(POSITIONAL_FIELD);
                    if (lastRecordOffset != null && !(lastRecordOffset instanceof Long))
                        throw new ConnectException("Offset position is the incorrect type");
                    if (lastRecordOffset != null) {
                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordOffset);
                        long skipLeft = (Long) lastRecordOffset;
                        while (skipLeft > 0) {
                            try {
                                long skipped = stream.skip(skipLeft);
                                skipLeft -= skipped;
                            } catch (IOException e) {
                                log.error("Error while trying to see to previous offset in file: ", e);
                                throw new ConnectException(e);
                            }
                        }
                    log.debug("Skipped to offset {}", lastRecordOffset);
                    }
                    streamOffset = (lastRecordOffset != null) ? (Long) lastRecordOffset : 0L;
                } else {
                    streamOffset = 0L;
                }
                reader = new BufferedReader(new InputStreamReader(stream));
                log.debug("Opened {} for reading", logFilename());
            } catch (FileNotFoundException e) {
                log.warn("Couldn't find file for FileSourceTasks, sleeping to wait for it to be created");
                synchronized (this) {
                    this.wait(1000);
                }
                return null;
            }
        }

        try {
            final BufferedReader readerCopy;
            synchronized (this) {
                readerCopy = reader;
            }
            if (readerCopy == null)
                return null;

            ArrayList<SourceRecord> records = null;

            int nread = 0;
            while (readerCopy.ready()) {
                nread = readerCopy.read(buffer, offset, buffer.length - offset);
                log.trace("Read {} bytes form {}", nread, logFilename());

                if (nread > 0) {
                    offset += nread;
                    if (offset == buffer.length) {
                        char[] newbuf = new char[buffer.length * 2];
                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                        buffer = newbuf;
                    }

                    String line;
                    do {
                        line = extractLine();
                        if (line != null) {
                            log.trace("Read a line from {}", logFilename());
                            if (records == null)
                                records = new ArrayList<>();
                            records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic, VALUE_SCHEMA, line));

                        }
                        new ArrayList<SourceRecord>();
                    } while (line != null);
                }
            }

            if (nread <=0)
                synchronized (this) {
                    this.wait(1000);
                }

            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }


        return null;
/*        try {
            ArrayList<SourceRecord> records = new ArrayList<>();
            while (streamValid(stream) && records.isEmpty()) {
                LineAndOffset line = readToNextLine(stream);
                if (line != null) {
                    Map sourcePartition = Collections.singletonMap("filename",filename);
                    Map sourceOffset = Collections.singletonMap("position", streamOffset);
                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line));
                } else {
                    Thread.sleep(1);
                }
            }
            return records;
        } catch (IOException e) {

        }*/

    }


    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //try {
            ArrayList<SourceRecord> records = new ArrayList<>();
            //while (streamValid(stream) && records.isEmpty()) {
                //LineAndOffset line = readToNextLine(stream);
                String line ;
                //
                Long timestamp = System.currentTimeMillis();
                //HttpResponse response = new HttpGetResource().http(url.toString(), "");
                line = new HttpGetResource().http(url.toString(), "");
                Thread.sleep(100);
                if (line != null) {
                    Map sourcePartition = Collections.singletonMap("URL", url.toString());
                    // Somehow get a position maybe a timestamp

                    Map sourceOffset = Collections.singletonMap("timestamp", timestamp);
                    records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line));
                    Thread.sleep(100);
                } else {
                    Thread.sleep(1);
                }
            //}
            return records;

       // } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        //return null;
    //}


    public String extractLine() {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset ; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i +2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            if (streamOffset != null)
                streamOffset += newStart;
            return result;
        } else {
            return null;
        }
    }


    @Override
    public synchronized void stop() {
        log.trace("Stopping");
        synchronized (this){
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    log.trace("Closing input stream");
                }
            } catch (IOException e) {
                log.error("Failed to close FileSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITIONAL_FIELD, pos);
    }

    private String logFilename() {
        return filename == null ? "stdin" : filename;
    }
}
