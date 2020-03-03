package com.espertech.esper.experiments;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EPException;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.common.client.util.NameAccessModifier;
import com.espertech.esper.common.internal.event.map.MapEventBean;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import no.uio.ifi.ExperimentAPI;
import no.uio.ifi.SpeComm;
import no.uio.ifi.TracingFramework;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class EsperExperimentFramework implements ExperimentAPI {
    private static final String EVENTTYPE = "";
    private volatile boolean keepRunning = true;
    private volatile int totalEventCount = 0;
    private volatile int threadCnt = 0;
    private int batch_size;
    private int interval_wait;
    private Map<String, Object> json_configuration;
    private int totalPkts;
    private int pktsPublished;
    private long timeLastRecvdTuple = 0;
    public TracingFramework tf = new TracingFramework();
    private int number_threads = 1;
    List<StreamListener> streamListeners = new ArrayList<>();

    TCPNettyServer tcpNettyServer;
    Map<Integer, TCPNettyClient> nodeIdToClient = new HashMap<>();
    Map<String, Integer> streamNameToId = new HashMap<>();
    Map<Integer, Map<String, Object>> nodeIdToIpAndPort = new HashMap<>();
    Map<Integer, List<Integer>> streamIdToNodeIds = new HashMap<>();
    Map<Integer, List<Map<String, Object>>> datasetIdToTuples = new HashMap<>();
    int port;

    ArrayList<Map<String, Object>> allPackets = new ArrayList<>();
    ArrayList<Integer> eventIDs = new ArrayList<>();

    ArrayList<String> queries = new ArrayList<>();
    Map<Integer, Map<String, Object>> allSchemas = new HashMap<>();
    private String trace_output_folder;

    public void SetTraceOutputFolder(String f) {this.trace_output_folder = f;}

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
        List<Map<String, Object>> attributes = (ArrayList<Map<String, Object>>) tuple.get("attributes");
        Map<String, Object> esper_attributes = new HashMap<>();
        int stream_id = (int) tuple.get("stream-id");

        for (Map<String, Object> attribute : attributes) {
            esper_attributes.put((String) attribute.get("name"), attribute.get("value"));
        }
        esper_attributes.put("stream-id", stream_id);

        for (int i = 0; i < quantity; i++) {
            allPackets.add(esper_attributes);
        }
        return "Success";
    }

    @Override
    public String ProcessDataset(Map<String, Object> ds) {
        System.out.println("ProcessDataset: ready to transmit tuples from dataset in file " + ds.get("file"));
        int stream_id = (int) ds.get("stream-id");
        Map<String, Object> schema = allSchemas.get(stream_id);
        List<Map<String, Object>> tuples = readTuplesFromDataset(ds, schema);
        System.out.println("Tuples are now read from file");
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

    @Override
    public String AddDataset(Map<String, Object> ds) {
        int stream_id = (int) ds.get("stream-id");
        Map<String, Object> schema = allSchemas.get(stream_id);

        if (ds.get("type").equals("csv")) {
            List<String> fields = new ArrayList<>();
            List<Class<?>> attr_types = new ArrayList<>();
            List<Map<String, Object>> tuple_format = (ArrayList<Map<String, Object>>) schema.get("tuple-format");
            for (Map<String, Object> stringObjectMap : tuple_format) {
                if (stringObjectMap.get("type").equals("string")) {
                    attr_types.add(String.class);
                } else if (stringObjectMap.get("type").equals("bool")) {
                    attr_types.add(Boolean.class);
                } else if (stringObjectMap.get("type").equals("int")) {
                    attr_types.add(Integer.class);
                } else if (stringObjectMap.get("type").equals("float")) {
                    attr_types.add(Float.class);
                } else if (stringObjectMap.get("type").equals("double")) {
                    attr_types.add(Double.class);
                } else if (stringObjectMap.get("type").equals("long")) {
                    attr_types.add(Long.class);
                } else if (stringObjectMap.get("type").equals("number")) {
                    attr_types.add(Float.class);
                } else if (stringObjectMap.get("type").equals("timestamp")) {
                    attr_types.add(String.class);
                } else if (stringObjectMap.get("type").equals("long-timestamp")) {
                    attr_types.add(Long.class);
                } else {
                    throw new RuntimeException("Invalid attribute type in dataset definition");
                }

                fields.add((String) stringObjectMap.get("name"));
            }

            try {
                BufferedReader csvReader = new BufferedReader(new FileReader((String) ds.get("file")));

                Map<String, Object> esper_attributes = new HashMap<>();
                String row;
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(",");
                    Object attr;
                    for (int i = 0; i < data.length; i++) {
                        if (attr_types.get(i) == String.class) {
                            attr = data[i];
                        } else if (attr_types.get(i) == Float.class) {
                            attr = Float.parseFloat(data[i]);
                        } else {
                            throw new RuntimeException("Invalid attribute type in dataset definition");
                        }
                        esper_attributes.put(fields.get(i), attr);
                    }
                    esper_attributes.put("stream-id", stream_id);
                    allPackets.add(esper_attributes);
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
                        // Not using rowtime yet, and Timestamp class not supported in schema
                        //attribute.put("value", new Timestamp(Long.parseLong(attribute.get("value").toString())));
                        attribute.put("value", Long.parseLong(attribute.get("value").toString()));
                        break;
                    case "timestamp":
                        // Not using rowtime yet, and Timestamp class not supported in schema
                        //attribute.put("value", Timestamp.valueOf(attribute.get("value").toString()));
                        attribute.put("value", Long.parseLong(attribute.get("value").toString()));
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
            Map<String, Object> map = yaml.load(fis);
            tuples = (ArrayList<Map<String, Object>>) map.get("cepevents");
            CastTuplesCorrectTypes(tuples, schema);
            datasetIdToTuples.put(stream_id, tuples);
        }
        return tuples;
    }

    void ProcessTuple(int stream_id, String stream_name, Map<String, Object> mappedBean) {
        if (streamIdToNodeIds.containsKey(stream_id)) {
            for (Integer nodeId : streamIdToNodeIds.get(stream_id)) {
                TCPNettyClient tcpNettyClient = nodeIdToClient.get(nodeId);
                if (tcpNettyClient == null) {
                    tcpNettyClient = new TCPNettyClient(true, true);
                    nodeIdToClient.put(nodeId, tcpNettyClient);
                    try {
                        for (int nid : nodeIdToIpAndPort.keySet()) {
                            if (nodeId.equals(nid)) {
                                Map<String, Object> addrAndPort = nodeIdToIpAndPort.get(nid);
                                tcpNettyClient.connect((String) addrAndPort.get("ip"), (int) addrAndPort.get("port"));
                                break;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(6);
                    }
                }

                try {
                    tcpNettyClient.send(stream_name, mappedBean).await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(6);
                }
            }
        }
    }

    int tupleCnt = 0;
    @Override
    public String AddStreamSchemas(List<Map<String, Object>> schemas) {
        for (Map<String, Object> schema : schemas) {
            int stream_id = (int) schema.get("stream-id");
            allSchemas.put(stream_id, schema);
            String stream_name = (String) schema.get("name");
            streamNameToId.put(stream_name, stream_id);

            StreamListener sl = new StreamListener() {
                @Override
                public String getChannelId() {
                    return stream_name;
                }

                @Override
                public void onMessage(Map<String, Object> message) {
                    if (++tupleCnt % 10000 == 0) {
                        System.out.println("Received tuple " + tupleCnt + ": " + message);
                    }
                    onEvent(message);
                }

                public void onEvent(Map<String, Object> mappedBean) {
                    timeLastRecvdTuple = System.currentTimeMillis();
                    try {
                        tf.traceEvent(1, new Object[]{Thread.currentThread().getId()});
                        runtime.getEventService().sendEventMap(mappedBean, stream_name);
                        tf.traceEvent(100, new Object[]{Thread.currentThread().getId()});
                    } catch (EPException e) {
                        e.printStackTrace();
                    }
                }
            };

            this.streamListeners.add(sl);
            this.tcpNettyServer.addStreamListener(sl);
        }
        return "Success";
    }

    Configuration config = new Configuration();
    CompilerArguments args;
    EPRuntime runtime;
    EPEventService eventService;
    EPCompiler compiler;
    boolean isRuntimeActive = false;

    @Override
    public String AddQueries(Map<String, Object> json_query) {
        String query = (String) ((Map<String, Object>) json_query.get("sql-query")).get("esper");
        int query_id = (int) json_query.get("id");
        tf.traceEvent(221, new Object[]{query_id});
        queries.add(query);
        return "Success";
    }

    int number_complex_events = 0;
    class ExperimentListener implements UpdateListener {
        public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
            if (++number_complex_events % 100000 == 0) {
                System.out.println("Produced " + number_complex_events + " complex events");
            }
            //System.out.println("ExperimentListener called with " + newEvents.length + " new events");
            for (EventBean bean : newEvents) {
                String stream_name = bean.getEventType().getMetadata().getName();
                //System.out.println("ExperimentListener.update, here we forward the complex event of stream " + stream_name);
                int stream_id = streamNameToId.get(stream_name);
                ProcessTuple(stream_id, stream_name, ((MapEventBean) bean).getProperties());
                tf.traceEvent(6);
            }
        }
    }

    @Override
    public String ProcessTuples(int number_tuples) {
        //System.out.println("Processing tuples");

        if (allPackets.isEmpty()) {
            String ret = "No tuples to process";
            System.out.println(ret);
            return ret;
        }
        for (int i = 0; i < number_tuples; i++) {
            Map<String, Object> mappedBean = allPackets.get(i % allPackets.size());
            Integer stream_id = (Integer) mappedBean.get("stream-id");
            String stream_name = (String) allSchemas.get(stream_id).get("name");
            ProcessTuple(stream_id, stream_name, mappedBean);
        }

        pktsPublished = 0;
        return "Success";
    }

    @Override
    public String RunEnvironment() {
        StringBuilder allQueries = new StringBuilder();
        for (String q : queries) {
            allQueries.append(q).append("\n");
        }

        StringBuilder schemas_string = new StringBuilder();
        for (Map<String, Object> schema : allSchemas.values()) {
            schemas_string.append(schema.get("esper")).append("\n");
        }

        try {
            runtime.getDeploymentService().undeployAll();
        } catch (EPUndeployException e) {
            e.printStackTrace();
        }
        try {
            EPCompiled compiled = compiler.compile(schemas_string.toString() + allQueries.toString(), args);
            ExperimentListener listener = new ExperimentListener();
            EPStatement[] stmts = runtime.getDeploymentService().deploy(compiled).getStatements();
            for (EPStatement stmt : stmts) {
                stmt.addListener(listener);
            }
        } catch (EPCompileException | EPDeployException | EPException e) {
            e.printStackTrace();
        }
        timeLastRecvdTuple = 0;
        return "Success";
    }

    @Override
    public String StopEnvironment() {
        tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
        return "Success";
    }

    //@Override
    //public void setNodeId(int nodeId) {this.nodeId = nodeId;}

    public void SetupClientTcpServer(int port) {
        this.tcpNettyServer = new TCPNettyServer();
        this.port = port;
        ServerConfig sc = new ServerConfig();
        sc.setPort(port);
        tcpNettyServer.start(sc);
    }

    @Override
    public String SetNodeIdToAddress(Map<Integer, Map<String, Object>> newNodeIdToIpAndPort) {
        nodeIdToIpAndPort = newNodeIdToIpAndPort;
        System.out.println("SetNodeIdToAddress, node Id - IP and port: " + newNodeIdToIpAndPort);
        return "Success";
    }

    @Override
    public String AddSubscriberOfStream(int streamId, int nodeId) {
        if (!streamIdToNodeIds.containsKey(streamId)) {
            streamIdToNodeIds.put(streamId, new ArrayList<>());
        }
        streamIdToNodeIds.get(streamId).add(nodeId);
        return "Success";
    }

    @Override
    public String ClearQueries() {
        tf.traceEvent(222);
        if (runtime == null) {
            return "Runtime hasn't been initialized";
        }
        try {
            runtime.getDeploymentService().undeployAll();
        } catch (EPUndeployException e) {
            e.printStackTrace();
        }
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
        long time_diff;
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
        System.out.println("Configure");
        // We only add schemas once
        isRuntimeActive = true;
        //File configFile = new File("/home/espen/Research/PhD/Private-WIP/stream-processing-benchmark-wip/" +
        //        "Experiments/experiment-configurations/SPE-specific-files/esper/movsim.esper.cfg.xml");
        //config.configure(configFile);
        config.getRuntime().getThreading().setInternalTimerEnabled(false);
        config.getRuntime().getThreading().setListenerDispatchPreserveOrder(false);
        runtime = EPRuntimeProvider.getRuntime("EsperExperimentFramework", config);
        runtime.initialize();
        eventService = runtime.getEventService();
        compiler = EPCompilerProvider.getCompiler();
        // types and variables shall be available for other statements
        config.getCompiler().getByteCode().setAccessModifierEventType(NameAccessModifier.PUBLIC);
        config.getCompiler().getByteCode().setAccessModifierVariable(NameAccessModifier.PUBLIC);

        // types shall be available for "sendEvent" use
        config.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);

        args = new CompilerArguments(config);
        args.getPath().add(runtime.getRuntimePath());
        return "Success";
    }

    public static void main(String[] args) {
        boolean continue_running = true;
        while (continue_running) {
            EsperExperimentFramework experimentAPI = new EsperExperimentFramework();
            SpeComm speComm = new SpeComm(args, experimentAPI);
            experimentAPI.SetupClientTcpServer(speComm.GetClientPort());
            experimentAPI.SetTraceOutputFolder(speComm.GetTraceOutputFolder());
            speComm.AcceptTasks();
        }
    }
}
