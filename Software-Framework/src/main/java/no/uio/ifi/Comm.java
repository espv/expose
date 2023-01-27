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
        String yaml_event = StringEscapeUtils.unescapeJava(received);
        Map<String, Object> map;
        synchronized (yaml) {
            map = yaml.load(yaml_event);
        }
        return map;
    }

    public void SendMap(Map<String, Object> map, PrintWriter writer) {
        String raw_yaml;
        synchronized (yaml) {
            raw_yaml = yaml.dump(map);
        }
        String yaml_event = StringEscapeUtils.escapeJava(raw_yaml) + "\n";
        writer.print(yaml_event);
        writer.flush();
    }
}
