package com.walmart.qarth.kafka.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by vgodbol on 4/21/16.
 */
public class HttpSourceConnector extends SourceConnector{
    public static final String TOPIC_CONFIG = "topic";
    public static final String HTTP_URL = "http";
    public static final String HTTP_METHOD = "GET"; // JUST ASSUME GET AS OF NOW
    public static final String FILE_CONFIG = "file";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILE_CONFIG, Type.STRING, Importance.HIGH,"Source filename")
            .define(HTTP_URL, Type.STRING, Importance.HIGH, "Source HTTP URL Return some json")
            .define(HTTP_METHOD, Type.STRING, Importance.HIGH,"Source HTTP METHOD - JUST A PLACEHOLDER AS OF NOW")
            .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "Topic to publish to");

    private String filename;
    private InputStream stream;
    private String topic;
    private URL url;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

        try {
            url = new URL(props.get(HttpSourceConnector.HTTP_URL));
        } catch (MalformedURLException e) {
            throw new ConnectException("HttpSourceConnector could not connect to the url", e);
        }


        filename = props.get(HttpSourceConnector.FILE_CONFIG);
        topic = props.get(HttpSourceConnector.TOPIC_CONFIG);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("FileSOURCEConnector config must have topic");
        if (topic.contains(","))
            throw new ConnectException("FileSOURCEconnector should have only 1 topic");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        if (url != null)
            config.put(HTTP_URL, url.toString());
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {

    }

    public static ConfigDef config() {
        return CONFIG_DEF;
    }
}
