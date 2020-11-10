package no.uio.ifi;

import org.apache.commons.text.StringEscapeUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class Comm {
    final Yaml yaml = new Yaml();

    static Map<String, Object> receiveMap(BufferedReader reader, Yaml yaml) throws IOException {
        String received = reader.readLine();
        System.out.println("Received map " + received);
        String yaml_event = StringEscapeUtils.unescapeJava(received);
        System.out.println("Received yaml event " + yaml_event);
        Map<String, Object> map;
        System.out.println("Before loading yaml");
        synchronized (yaml) {
            map = yaml.load(yaml_event);
        }
        System.out.println("After loading yaml");
        return map;
    }

    public void SendMap(Map<String, Object> map, PrintWriter writer) {
        System.out.println("Sending map " + map);
        String raw_yaml;
        synchronized (yaml) {
            raw_yaml = yaml.dump(map);
        }
        System.out.println("Sending raw_yaml " + raw_yaml);
        String yaml_event = StringEscapeUtils.escapeJava(raw_yaml) + "\n";
        System.out.println("Sending yaml_event " + yaml_event);
        writer.print(yaml_event);
        writer.flush();
    }
}
