package io.siddhi.experiments;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.experiments.utils.Tuple2;
import io.siddhi.experiments.utils.Tuple3;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import no.uio.ifi.ExperimentAPI;
import no.uio.ifi.SpeComm;
import no.uio.ifi.TracingFramework;
import org.wso2.extension.siddhi.map.binary.sinkmapper.BinaryEventConverter;
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class SiddhiExperimentFramework implements ExperimentAPI {
    private volatile int totalEventCount = 0;
    private volatile int threadCnt = 0;
    private int batch_size;
    private int interval_wait;
    private int pktsPublished;
    private TracingFramework tf = new TracingFramework();
    int number_threads = 1;
    long timeLastRecvdTuple = 0;
    SiddhiAppRuntime siddhiAppRuntime;

    TCPNettyServer tcpNettyServer;
    List<StreamListener> streamListeners = new ArrayList<>();
    Map<Integer, TCPNettyClient> nodeIdToClient = new HashMap<>();
    Map<String, Integer> streamNameToId = new HashMap<>();
    Map<Integer, Map<String, Object>> nodeIdToIpAndPort = new HashMap<>();
    Map<Integer, List<Map<String, Object>>> datasetIdToTuples = new HashMap<>();

    ArrayList<Tuple3<byte[], Attribute.Type[], String>> allPackets = new ArrayList<>();
    ArrayList<Tuple2<String, StreamCallback>> allCallbacks = new ArrayList<>();
    ArrayList<Integer> eventIDs = new ArrayList<>();

    SiddhiManager siddhiManager = new SiddhiManager();
    StringBuilder queries = new StringBuilder();
    Map<Integer, Map<String, Object>> allSchemas = new HashMap<>();
    Map<Integer, List<Integer>> schemaToNodeIds = new HashMap<>();
    int port;
    private String trace_output_folder;

    //@Override
    public String SetupClientTcpServer(int port) {
        this.tcpNettyServer = new TCPNettyServer();
        this.port = port;
        ServerConfig sc = new ServerConfig();
        sc.setPort(port);
        tcpNettyServer.start(sc);
        return "Success";
    }

    public void TearDownTcpServer() {
        //this.tcpNettyServer.shutdownGracefully();
    }

    public void SetTraceOutputFolder(String f) {this.trace_output_folder = f;}

    @Override
    public String RunEnvironment() {
        timeLastRecvdTuple = 0;
        StartSiddhiAppRuntime();
        return "Success";
    }

    @Override
    public String StopEnvironment() {
        siddhiAppRuntime.shutdown();
        tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
        return "Success";
    }

    @Override
    public String SetTupleBatchSize(int size) {
        batch_size = size;
        return "Success";
    }

    @Override
    public String SetIntervalBetweenTuples(int interval) {
        interval_wait = interval;
        return "Success";
    }

    @Override
    public String AddTuples(Map<String, Object> tuple, int quantity) {
        int stream_id = (int) tuple.get("stream-id");
        Map<String, Object> schema = allSchemas.get(stream_id);
        String stream_name = (String) schema.get("name");
        List<Map<String, Object>> attributes = (ArrayList<Map<String, Object>>) tuple.get("attributes");
        Object[] siddhi_attributes = new Object[attributes.size()];
        Attribute.Type[] streamType = (Attribute.Type[]) schema.get("stream-type");

        for (int i = 0; i < attributes.size(); i++) {
            Map<String, Object> attribute = attributes.get(i);
            siddhi_attributes[i] = attribute.get("value");
        }

        try {
            for (int i = 0; i < quantity; i++) {
                tf.traceEvent(220, new Object[]{/*event_id*/});
                Event[] events = new Event[1];
                events[0] = new Event(System.currentTimeMillis(), siddhi_attributes);
                allPackets.add(new Tuple3<>(BinaryEventConverter.convertToBinaryMessage(events, streamType).array(), streamType, stream_name));
                //eventIDs.add(event_id);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Success";
    }

    @Override
    public String AddDataset(Map<String, Object> ds) {
        int stream_id = (int) ds.get("stream-id");
        Map<String, Object> schema = allSchemas.get(stream_id);

        if (ds.get("type").equals("csv")) {
            List<Class<?>> attr_types = new ArrayList<>();
            String stream_name = (String) schema.get("name");
            List<Map<String, Object>> tuple_format = (ArrayList<Map<String, Object>>) schema.get("tuple-format");
            for (Map<String, Object> attribute : tuple_format) {
                if (attribute.get("type").equals("string")) {
                    attr_types.add(String.class);
                } else if (attribute.get("type").equals("bool")) {
                    attr_types.add(Boolean.class);
                } else if (attribute.get("type").equals("int")) {
                    attr_types.add(Integer.class);
                } else if (attribute.get("type").equals("float")) {
                    attr_types.add(Float.class);
                } else if (attribute.get("type").equals("double")) {
                    attr_types.add(Double.class);
                } else if (attribute.get("type").equals("int")) {
                    attr_types.add(Integer.class);
                } else if (attribute.get("type").equals("number")) {
                    attr_types.add(Float.class);
                } else if (attribute.get("type").equals("timestamp")) {
                    // I don't know how to add external timestamp
                    attr_types.add(String.class);
                } else if (attribute.get("type").equals("long-timestamp")) {
                    // I don't know how to add external timestamp
                    attr_types.add(Long.class);
                } else {
                    throw new RuntimeException("Invalid attribute type in dataset definition");
                }
            }

            Attribute.Type[] streamType = (Attribute.Type[]) schema.get("stream-type");
            int cnt = 0;
            try {
                BufferedReader csvReader = new BufferedReader(new FileReader((String) ds.get("file")));

                String row;
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(",");
                    Object attr;
                    Object[] siddhi_attributes = new Object[data.length];
                    for (int i = 0; i < data.length; i++) {
                        if (attr_types.get(i) == String.class) {
                            attr = data[i];
                        } else if (attr_types.get(i) == Boolean.class) {
                            attr = Boolean.parseBoolean(data[i]);
                        } else if (attr_types.get(i) == Integer.class) {
                            attr = Integer.parseInt(data[i]);
                        } else if (attr_types.get(i) == Double.class) {
                            attr = Double.parseDouble(data[i]);
                        } else if (attr_types.get(i) == Long.class) {
                            attr = Long.parseLong(data[i]);
                        } else if (attr_types.get(i) == Float.class) {
                            attr = Float.parseFloat(data[i]);
                        } else {
                            throw new RuntimeException("Invalid attribute type in dataset definition");
                        }
                        siddhi_attributes[i] = attr;
                    }
                    eventIDs.add(cnt++);
                    Event[] events = new Event[1];
                    events[0] = new Event(System.currentTimeMillis(), siddhi_attributes);
                    allPackets.add(new Tuple3<>(BinaryEventConverter.convertToBinaryMessage(events, streamType).array(), streamType, stream_name));
                }
                csvReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (ds.get("type").equals("yaml")) {
            List<Map<String, Object>> tuples = readTuplesFromDataset(ds, schema);
            for (Map<String, Object> tuple : tuples) {
                AddTuples(tuple, 1);
            }
        } else {
            throw new RuntimeException("Invalid dataset type for dataset with Id " + ds.get("id"));
        }
        return "Success";
    }

    @Override
    public String ProcessDataset(Map<String, Object> ds) {
        int stream_id = (int) ds.get("stream-id");
        Map<String, Object> schema = allSchemas.get(stream_id);
        List<Map<String, Object>> tuples = readTuplesFromDataset(ds, schema);
        double prevTimestamp = 0;
        long prevTime = System.nanoTime();
        for (Map<String, Object> tuple : tuples) {
            allPackets.clear();
            AddTuples(tuple, 1);
            if ((boolean) ds.getOrDefault("realism", false) && schema.containsKey("rowtime-column")) {
                Map<String, Object> rowtime_column = (Map<String, Object>) schema.get("rowtime-column");
                double timestamp = 0;
                for (Map<String, Object> attribute : (List<Map<String, Object>>) tuple.get("attributes")) {
                    if (attribute.get("name").equals(rowtime_column.get("column"))) {
                        int nanoseconds_per_tick = (int) rowtime_column.get("nanoseconds-per-tick");
                        timestamp = (double) attribute.get("value") * nanoseconds_per_tick;
                        if (prevTimestamp == 0) {
                            prevTimestamp = timestamp;
                        }
                        break;
                    }
                }
                double time_diff_tuple = timestamp - prevTimestamp;
                long time_diff_real = System.nanoTime() - prevTime;
                while (time_diff_real < time_diff_tuple) {
                    time_diff_real = System.nanoTime() - prevTime;
                }

                prevTimestamp = timestamp;
                prevTime = System.nanoTime();
            }
            if (!allPackets.isEmpty()) {
                ProcessTuples(1);
            }
        }
        return "Success";
    }

    void CastTuplesCorrectTypes(List<Map<String, Object>> tuples, Map<String, Object> schema) {
        List<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
        for (Map<String, Object> tuple : tuples) {
            List<Map<String, Object>> attributes = (List<Map<String, Object>>) tuple.get("attributes");
            for (int i = 0; i < tuple_format.size(); i++) {
                Map<String, String> attribute_format = tuple_format.get(i);
                Map<String, Object> attribute = attributes.get(i);
                switch (attribute_format.get("type")) {
                    case "string":
                        attribute.put("value", attribute.get("value").toString());
                        break;
                    case "bool":
                        attribute.put("value", Boolean.valueOf(attribute.get("value").toString()));
                        break;
                    case "int":
                        attribute.put("value", Integer.parseInt(attribute.get("value").toString()));
                        break;
                    case "float":
                        attribute.put("value", Float.parseFloat(attribute.get("value").toString()));
                        break;
                    case "double":
                        attribute.put("value", Double.parseDouble(attribute.get("value").toString()));
                        break;
                    case "long":
                        attribute.put("value", Long.parseLong(attribute.get("value").toString()));
                        break;
                    case "long-timestamp":
                        // I don't know how to add external timestamp
                        attribute.put("value", Long.parseLong(attribute.get("value").toString()));
                        break;
                    case "timestamp":
                        // I don't know how to add external timestamp
                        attribute.put("value", attribute.get("value").toString());
                        break;
                    default:
                        throw new RuntimeException("Invalid attribute type in dataset definition");
                }
            }
        }
    }

    private List<Map<String, Object>> readTuplesFromDataset(Map<String, Object> ds, Map<String, Object> schema) {
        int stream_id = (int) ds.get("stream-id");
        List<Map<String, Object>> tuples = datasetIdToTuples.get(stream_id);
        if (tuples == null) {
            FileInputStream fis = null;
            Yaml yaml = new Yaml();
            try {
                fis = new FileInputStream((String) ds.get("file"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            Map<String, Object> map = (Map<String, Object>) yaml.load(fis);
            tuples = (ArrayList<Map<String, Object>>) map.get("cepevents");
            CastTuplesCorrectTypes(tuples, schema);
            datasetIdToTuples.put(stream_id, tuples);
        }
        return tuples;
    }

    @Override
    public String AddSubscriberOfStream(int streamId, int nodeId) {
        if (!schemaToNodeIds.containsKey(streamId)) {
            schemaToNodeIds.put(streamId, new ArrayList<>());
        }
        schemaToNodeIds.get(streamId).add(nodeId);
        return "Success";
    }

    @Override
    public String SetNodeIdToAddress(Map<Integer, Map<String, Object>> newNodeIdToIpAndPort) {
        nodeIdToIpAndPort = newNodeIdToIpAndPort;
        return "Success";
    }

    public void ProcessTuple(int stream_id, String stream_name, Attribute.Type[] streamTypes, Event event) {
        if (schemaToNodeIds.containsKey(streamNameToId.get(stream_name))) {
            for (Integer nodeId : schemaToNodeIds.get(stream_id)) {
                TCPNettyClient tcpNettyClient = nodeIdToClient.get(nodeId);
                if (tcpNettyClient == null) {
                    tcpNettyClient = new TCPNettyClient(true, true);
                    nodeIdToClient.put(nodeId, tcpNettyClient);
                    for (int nid : nodeIdToIpAndPort.keySet()) {
                        if (nodeId.equals(nid)) {
                            Map<String, Object> addrAndPort = nodeIdToIpAndPort.get(nid);
                            tcpNettyClient.connect((String) addrAndPort.get("ip"), (int) addrAndPort.get("port"));
                            break;
                        }
                    }
                }

                List<Event> to_send = new ArrayList<>();
                to_send.add(event);
                try {
                    tcpNettyClient.send(stream_name, BinaryEventConverter.convertToBinaryMessage(
                            to_send.toArray(new Event[1]), streamTypes).array()).await();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    System.exit(6);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(7);
                }
            }
        }
    }

    int cnt2 = 0;
    int cnt3 = 0;
    @Override
    public String AddStreamSchemas(List<Map<String, Object>> schemas) {
        for (Map<String, Object> schema : schemas) {
            String stream_name = (String) schema.get("name");
            int stream_id = (int) schema.get("stream-id");
            streamNameToId.put(stream_name, stream_id);
            allSchemas.put(stream_id, schema);

            StreamDefinition streamDefinition = StreamDefinition.id(stream_name);
            for (Map<String, Object> j : (ArrayList<Map<String, Object>>) schema.get("tuple-format")) {
                Attribute.Type type;
                if (j.get("type").equals("string")) {
                    type = Attribute.Type.STRING;
                } else if (j.get("type").equals("bool")) {
                    type = Attribute.Type.BOOL;
                } else if (j.get("type").equals("int")) {
                    type = Attribute.Type.INT;
                } else if (j.get("type").equals("float")) {
                    type = Attribute.Type.DOUBLE;
                } else if (j.get("type").equals("double")) {
                    type = Attribute.Type.DOUBLE;
                } else if (j.get("type").equals("long")) {
                    type = Attribute.Type.LONG;
                } else if (j.get("type").equals("number")) {
                    type = Attribute.Type.FLOAT;
                } else if (j.get("type").equals("timestamp")) {
                    type = Attribute.Type.STRING;
                } else if (j.get("type").equals("long-timestamp")) {
                    type = Attribute.Type.LONG;
                } else {
                    throw new RuntimeException("Invalid attribute type in stream schema");
                }
                streamDefinition = streamDefinition.attribute((String) j.get("name"), type);
            }

            Attribute.Type[] streamTypes = EventDefinitionConverterUtil.generateAttributeTypeArray(
                    streamDefinition.getAttributeList());
            schema.put("stream-type", streamTypes);

            // Add stream listeners for the distributed scenario
            StreamDefinition finalStreamDefinition = streamDefinition;
            StreamListener sl = new StreamListener() {
                @Override
                public String getChannelId() {
                    return finalStreamDefinition.getId();
                }

                @Override
                public void onMessage(byte[] message) {
                    onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), streamTypes));
                }

                public void onEvents(Event[] events) {
                    for (Event event : events) {
                        onEvent(event);
                    }
                }

                public void onEvent(Event event) {
                    //LOG.info(event);
                    timeLastRecvdTuple = System.currentTimeMillis();
                    if (++cnt2 % 100000 == 0)
                        System.out.println("Received event number " + cnt2 + ": " + event);
                    try {
                        tf.traceEvent(1, new Object[]{Thread.currentThread().getId(), Event.running_id + 1/*, eventIDs.get((int)curPktsPublished%allPackets.size())*/});
                        siddhiAppRuntime.getInputHandler(finalStreamDefinition.getId()).send(event);
                        tf.traceEvent(100, new Object[]{Thread.currentThread().getId(), Event.running_id/*, eventIDs.get((int)curPktsPublished%allPackets.size())*/});
                        //ProcessTuple(stream_id, finalStreamDefinition.getId(), streamTypes, event);
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            };

            this.streamListeners.add(sl);
            this.tcpNettyServer.addStreamListener(sl);

            StreamCallback sc = new StreamCallback() {
                int eventCount = 0;

                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        cnt3 += events.length;
                        if (cnt3 % 100000 == 0)
                            System.out.println("Produced complex event " + event.GetId());
                        eventCount++;
                        tf.traceEvent(6, new Object[]{Thread.currentThread().getId()});

                        ProcessTuple(stream_id, stream_name, streamTypes, event);
                    }
                }
            };
            allCallbacks.add(new Tuple2<>(stream_name, sc));
        }

        StartSiddhiAppRuntime();
        return "Success";
    }

    private void StartSiddhiAppRuntime() {
        StringBuilder schemasString = new StringBuilder();
        for (Map<String, Object> s : allSchemas.values()) {
            if (s.containsKey("siddhi")) {
                schemasString.append(s.get("siddhi")).append("\n");
            }
        }
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(schemasString.toString() + "\n" + queries.toString());
        for (Tuple2<String, StreamCallback> t : allCallbacks) {
            siddhiAppRuntime.addCallback(t.getFirst(), t.getSecond());
        }
        siddhiAppRuntime.start();
    }

    @Override
    public String AddQueries(Map<String, Object> json_query) {
        String query = (String) ((Map<String, Object>) json_query.get("sql-query")).get("siddhi");
        int query_id = (int) json_query.get("id");
        //for (int i = 0; i < quantity; i++) {
        tf.traceEvent(221, new Object[]{query_id});
        queries.append(query).append("\n");
        //}

        return "Success";
    }

    @Override
    public String ProcessTuples(int number_tuples) {
        //System.out.println("Processing tuples " + (++cnt));
        if (allPackets.isEmpty()) {
            System.out.println("No tuples to process");
        }
        // Invoke all threads for this loop. Perhaps they have to wait until I wait a few milliseconds and unlock a mutex
        for (int i = 0; i < number_threads; i++) {
            //new Thread(() -> {
            while (pktsPublished < number_tuples) {
                if (allPackets.isEmpty()) {
                    break;
                }
                int curPktsPublished = pktsPublished;
                Tuple3<byte[], Attribute.Type[], String> t = allPackets.get(curPktsPublished % allPackets.size());
                ++pktsPublished;
                // Event.running_id+1 becomes the ID of the event that is created
                // We record the thread ID, running event Id and the base event Id
                Event[] events;
                events = SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(t.getFirst()), t.getSecond());
                for (Event event : events) {
                    int stream_id = streamNameToId.get(t.getThird());
                    ProcessTuple(stream_id, t.getThird(), t.getSecond(), event);
                }
            }
        }
        pktsPublished = 0;
        return "Success";
    }

    @Override
    public String ClearQueries() {
        queries.setLength(0);
        tf.traceEvent(222);
        return "Success";
    }

    @Override
    public String ClearTuples() {
        allPackets.clear();
        tf.traceEvent(223);
        return "Success";
    }

    @Override
    public String CleanupExperiment() {
        tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
        return "Success";
    }

    @Override
    public String AddTracepointIds(List<Object> tracepointIds) {
        for (int tracepointId : (List<Integer>) (List<?>) tracepointIds) {
            this.tf.addTracepoint(tracepointId);
        }
        return "Success";
    }

    @Override
    public String NotifyAfterNoReceivedTuple(int milliseconds) {
        long time_diff = 0;
        do {
            try {
                Thread.sleep(milliseconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            time_diff = System.currentTimeMillis() - timeLastRecvdTuple;
        } while (time_diff < milliseconds || timeLastRecvdTuple == 0);
        return Long.toString(time_diff);
    }

    @Override
    public String TraceTuple(int tracepointId, List<String> traceArguments) {
        tf.traceEvent(tracepointId, traceArguments.toArray());
        return "Success";
    }

    @Override
    public String Configure() {
        return "Success";
    }

    public static void main(String[] args) {
        boolean continue_running = true;
        while (continue_running) {
            SiddhiExperimentFramework experimentAPI = new SiddhiExperimentFramework();
            SpeComm speComm = new SpeComm(args, experimentAPI);
            experimentAPI.SetupClientTcpServer(speComm.GetClientPort());
            experimentAPI.SetTraceOutputFolder(speComm.GetTraceOutputFolder());
            speComm.AcceptTasks();
            experimentAPI.TearDownTcpServer();
        }
    }
}
