package no.uio.ifi;

import org.apache.commons.text.StringEscapeUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class Comm {
    Yaml yaml = new Yaml();

    static Map<String, Object> receiveMap(BufferedReader reader, Yaml yaml) throws IOException {
        String yaml_event = StringEscapeUtils.unescapeJava(reader.readLine());
        return yaml.load(yaml_event);
    }

    public void SendMap(Map<String, Object> map, PrintWriter writer) {
        String yaml_event = StringEscapeUtils.escapeJava(yaml.dump(map)) + "\n";
        writer.print(yaml_event);
        writer.flush();
    }
}
