package org.apache.flink.experiments;

import no.uio.ifi.ExperimentAPI;
import no.uio.ifi.SpeComm;
import no.uio.ifi.TracingFramework;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka09Fetcher;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

@SuppressWarnings("unchecked")
public class FlinkExperimentFramework implements ExperimentAPI, Serializable {
	static long timeLastRecvdTuple = 0;
	int batch_size;
	int pktsPublished;
	int interval_wait;
	String trace_output_folder;
	StreamExecutionEnvironment env;
	StreamTableEnvironment tableEnv;
	EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
	Map<Integer, ArrayList<Row> > streamToTuples = new HashMap<>();
	Map<Integer, ArrayList<Map<String, Object>> > outputStreamIdToFetchQueries = new HashMap<>();
	Map<Integer, ArrayList<Map<String, Object>> > outputStreamIdToUpdateQueries = new HashMap<>();
	ArrayList<Map<String, Object>> queries = new ArrayList<>();
	Map<Integer, Map<String, Object>> allSchemas = new HashMap<>();
	Map<Integer, TypeInformation<Row>> streamIdToTypeInfo = new HashMap<>();
	Map<Integer, TypeInformationSerializationSchema<Row>> streamIdToSerializationSchema = new HashMap<>();
	Map<String, Integer> streamNameToId = new HashMap<>();
	Map<Integer, String> streamIdToName = new HashMap<>();
	Map<Integer, Map<String, Object>> nodeIdToIpAndPort = new HashMap<>();
	Map<Integer, List<Integer>> streamIdToNodeIds = new HashMap<>();
	Map<Integer, DataStream<Row> > streamIdToDataStream = new HashMap<>();
	Map<Integer, List<Map<String, Object>>> datasetIdToTuples = new HashMap<>();
	Map<Integer, Boolean> printDataStream = new HashMap<>();
	static TracingFramework tf = new TracingFramework();
	Producer<String, byte[]> producer;
	Map<Integer, SourceFunction<Row>> envSourceFunctions = new HashMap<>();

	List<FlinkKafkaProducer010<Row> > sinkFunctions = new ArrayList<>();
	Map<Integer, SinkFunction<Row> > regularSinkFunctions = new HashMap<>();
	Map<String, FlinkKafkaProducer010<Row> > querySinkFunctions = new HashMap<>();
	//List<SourceFunction<Row> > sourceFunctions = new ArrayList<>();
	Thread threadRunningEnvironment;
	static int nodeId;
	Properties props;
	Map<Integer, KafkaProducer> nodeIdToKafkaProducer = new HashMap<>();
	Map<Integer, Properties> nodeIdToProperties = new HashMap<>();

	FlinkExperimentFramework() {
		props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		producer = new KafkaProducer<>(props);
	}

	public void setNodeId(int nodeId) {FlinkExperimentFramework.nodeId = nodeId;}

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
	public String SetNodeIdToAddress(Map<Integer, Map<String, Object> > newNodeIdToIpAndPort) {
		//System.out.println("SetNodeIdToAddress: " + newNodeIdToIpAndPort);
		nodeIdToIpAndPort = newNodeIdToIpAndPort;
		Properties properties = new Properties();
		for (Object key : props.keySet()) {
			properties.put(key, props.get(key));
		}

		for (int nodeId : nodeIdToIpAndPort.keySet()) {
			String ip = (String) nodeIdToIpAndPort.get(nodeId).get("ip");
			int port = (int) nodeIdToIpAndPort.get(nodeId).get("port");
			properties.put("bootstrap.servers", ip+":"+port);
			nodeIdToProperties.put(nodeId, properties);
			nodeIdToKafkaProducer.put(nodeId, new KafkaProducer<>(properties));
		}
		return "Success";
	}

	@Override
	public String AddSubscriberOfStream(int streamId, int nodeId) {
		if (!streamIdToNodeIds.containsKey(streamId)) {
			streamIdToNodeIds.put(streamId, new ArrayList<>());
		}

		streamIdToNodeIds.get(streamId).add(nodeId);
		for (Map<String, Object> schema : allSchemas.values()) {
			if (schema.get("stream-id").equals(streamId)) {
				String stream_name = (String) schema.get("name");
				DataStream<Row> ds = streamIdToDataStream.get(streamId);
				if (ds == null) {
					AddKafkaConsumer(schema);
				}
				ds = streamIdToDataStream.get(streamId);
				for (int otherNodeId : streamIdToNodeIds.get(streamId)) {
					String topic = stream_name + "-" + otherNodeId;
					FlinkKafkaProducer010<Row> p = new FlinkKafkaProducer010<>(topic, streamIdToSerializationSchema.get(streamId), nodeIdToProperties.get(otherNodeId));
					sinkFunctions.add(p);
					ds.addSink(p);
				}
				break;
			}
		}
		return "Success";
	}

	final static int[] cnt = {0};

	private static class TimestampsAndWatermarks extends BoundedOutOfOrdernessTimestampExtractor<Row> {
		public TimestampsAndWatermarks() {
			super(Time.milliseconds(1));
		}

		long cnt = 0;
		@Override
		public long extractTimestamp(Row event) {
			cnt += 1000000000;
			return cnt;
		}
	}

	private void AddKafkaConsumer(Map<String, Object> schema) {
		int stream_id = (int) schema.get("stream-id");
		String stream_name = (String) schema.get("name");
		String topic = stream_name + "-" + FlinkExperimentFramework.nodeId;
		//System.out.println("Subscribing to kafka topic " + topic);
		FlinkKafkaConsumer010<Row> consumer = new FlinkKafkaConsumer010<>(topic, streamIdToSerializationSchema.get(stream_id), this.props, tf);
		//sourceFunctions.add(consumer);
		DataStream<Row> ds = env.addSource(consumer).returns(streamIdToTypeInfo.get(stream_id)).assignTimestampsAndWatermarks(
			new AscendingTimestampExtractor<Row>() {
				@Override
				public long extractAscendingTimestamp(Row element) {
					if (!schema.containsKey("rowtime-column")) {
						return 0;
					}
					List<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
					for (int i = 0; i < element.getArity(); i++) {
						if (tuple_format.get(i).get("name").equals(((Map<String, Object>) schema.get("rowtime-column")).get("column"))) {
							return ((Timestamp) element.getField(i)).getTime();
						}
					}
					return 0;
				}
			});
			/*new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.milliseconds(1)) {
			  @Override
			  public long extractTimestamp(Row element) {
				  if (!schema.containsKey("rowtime-column")) {
					  return 0;
				  }
				  List<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
				  for (int i = 0; i < element.getArity(); i++) {
					  if (tuple_format.get(i).get("name").equals(((Map<String, Object>) schema.get("rowtime-column")).get("column"))) {
						  return ((Timestamp) element.getField(i)).getTime();
					  }
				  }
				  return 0;
			  }
			});*/ /*new TimestampsAndWatermarks());*//*new
																																  AssignerWithPeriodicWatermarks<Row>() {
																																	  private long maxTimestampSeen = 0;
																																	  long cnt = 0;

																																	  @Override
																																	  public Watermark getCurrentWatermark() {
																																		  return new Watermark(maxTimestampSeen);
																																	  }

																																	  @Override
																																	  public long extractTimestamp(Row temperatureEvent, long l)
																																	  {
																																		  //long ts = temperatureEvent.getTimestamp();
																																		  // if (temperatureEvent.getKey().equals("W"))
																																		  //maxTimestampSeen = Long.max(maxTimestampSeen,ts);
																																		  //return ts;
																																		  cnt += 1000000000;
																																		  //System.out.println("Time: " + cnt + ", topic: " + topic);
																																		  return cnt;
																																	  }
																																  })*/ // Add as many fields as your Row has;
			//envSourceFunctions.put(stream_id, consumer);

			//if (nodeId == 4)
			//ds.print();
		SinkFunction<Row> sf = new SinkFunction<Row>() {
			@Override
			public void invoke(Row value) throws Exception {
				if (++cnt[0] % 10000 == 0)
					System.out.println( System.nanoTime() + ": Received tuple " + cnt[0]);
			}
		};
		regularSinkFunctions.put(stream_id, sf);
		ds.addSink(sf);
		StringBuilder fields = new StringBuilder();
		for (Map<String, String> attribute : (List<Map<String, String> >) schema.get("tuple-format")) {
			if (!fields.toString().equals("")) {
				fields.append(", ");
			}

			fields.append(attribute.get("name"));
		}
		tableEnv.registerDataStream(stream_name, ds, fields.toString());
		this.streamIdToDataStream.put(stream_id, ds);
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
						attribute.put("value", new Timestamp(Long.parseLong(attribute.get("value").toString())));
						break;
					case "timestamp":
						attribute.put("value", Timestamp.valueOf(attribute.get("value").toString()));
						break;
					default:
						throw new RuntimeException("Invalid attribute type in dataset definition");
				}
			}
		}
	}

	@Override
	public String ProcessDataset(Map<String, Object> ds) {
		//System.out.println("Processing dataset");
		int stream_id = (int) ds.get("stream-id");
		List<Map<String, Object>> tuples = datasetIdToTuples.get(stream_id);
		Map<String, Object> schema = allSchemas.get(stream_id);
		if (tuples == null) {
			Map<String, Object> map = GetMapFromYaml(ds);
			tuples = (ArrayList<Map<String, Object>>) map.get("cepevents");
			CastTuplesCorrectTypes(tuples, schema);
			datasetIdToTuples.put(stream_id, tuples);
		}
		double prevTimestamp = 0;
		//System.out.println("Ready to transmit tuples");
		long prevTime = System.nanoTime();
		boolean realism = (boolean) ds.getOrDefault("realism", false) && schema.containsKey("rowtime-column");
		for (Map<String, Object> tuple : tuples) {
			AddTuples(tuple, 1);

			if (realism) {
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
		}

		if (!realism) {
			ProcessTuples(tuples.size());
		}
		return "Success";
	}

	@Override
	public String AddTuples(Map<String, Object> tuple, int quantity) {
		ArrayList<Map<String, Object> > attributes = (ArrayList<Map<String, Object> >) tuple.get("attributes");
		Row new_tuple = new Row(attributes.size());
		for (int i = 0; i < attributes.size(); i++) {
			Map<String, Object> attribute = attributes.get(i);
			new_tuple.setField(i, attribute.get("value"));
		}
		int stream_id = (int) tuple.get("stream-id");
		if (!streamToTuples.containsKey(stream_id)) {
			streamToTuples.put(stream_id, new ArrayList<>());
		}

		for (int i = 0; i < quantity; i++) {
			streamToTuples.get(stream_id).add(new_tuple);
		}
		return "Success";
	}

	@Override
	public String AddStreamSchemas(List<Map<String, Object>> schemas) {
		for (Map<String, Object> schema : schemas) {
			int stream_id = (int) schema.get("stream-id");
			final String stream_name = (String) schema.get("name");
			streamIdToName.put(stream_id, stream_name);
			streamNameToId.put(stream_name, stream_id);
			allSchemas.put(stream_id, schema);
			ArrayList<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
			TypeInformation<?>[] typeInformations = new TypeInformation[tuple_format.size()];
			int pos = -1;
			for (Map<String, String> attribute : tuple_format) {
				pos += 1;
				switch (attribute.get("type")) {
					case "string":
						typeInformations[pos] = Types.STRING;
						break;
					case "bool":
						typeInformations[pos] = Types.BOOLEAN;
						break;
					case "int":
						typeInformations[pos] = Types.INT;
						break;
					case "float":
						typeInformations[pos] = Types.FLOAT;
						break;
					case "double":
						typeInformations[pos] = Types.DOUBLE;
						break;
					case "long":
						typeInformations[pos] = Types.LONG;
						break;
					case "long-timestamp":
					case "timestamp":
						typeInformations[pos] = Types.SQL_TIMESTAMP;
						break;
					default:
						throw new RuntimeException("Invalid attribute type in dataset definition");
				}
			}

			if (env == null) {
				env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.getConfig().disableSysoutLogging();
				env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
				tableEnv = StreamTableEnvironment.create(env, fsSettings);
			}

			streamIdToTypeInfo.put(stream_id, new RowTypeInfo(typeInformations));
			streamIdToSerializationSchema.put(stream_id, new TypeInformationSerializationSchema<>(streamIdToTypeInfo.get(stream_id), env.getConfig()));
		}
		return "Success";
	}

	int deployedQueries = 0;
	public void DeployQueries(int outputStreamId) {
		String outputStreamName = streamIdToName.get(outputStreamId);
		queries = outputStreamIdToFetchQueries.getOrDefault(outputStreamId, new ArrayList<>());
		for (Map<String, Object> q : queries) {
			String sql_query = ((Map<String, String>)q.get("sql-query")).get("flink");
			Table result = tableEnv.sqlQuery(sql_query);
			DataStream<Row> ds = tableEnv.toRetractStream(result, streamIdToTypeInfo.get(outputStreamId))
				.map((MapFunction<Tuple2<Boolean, Row>, Row>) value -> value.f1);

			if (streamIdToNodeIds.containsKey(outputStreamId)) {
				for (int otherNodeId : streamIdToNodeIds.get(outputStreamId)) {
					//System.out.println("Creating new FlinkKafkaProducer to write to the other node");
					String topic = outputStreamName + "-" + otherNodeId;
					FlinkKafkaProducer010<Row> p;
					if (querySinkFunctions.containsKey(topic)) {
						p = querySinkFunctions.get(topic);
					} else {
						p = new FlinkKafkaProducer010<>(topic, streamIdToSerializationSchema.get(outputStreamId), nodeIdToProperties.get(otherNodeId));

						querySinkFunctions.put(topic, p);
					}
					ds.addSink(p);
				}
			}
			/*} else {
				//System.out.println("Creating new FlinkKafkaProducer to write to myself");
				String topic = outputStreamName + "-" + FlinkExperimentFramework.nodeId;
				FlinkKafkaProducer010<Row> p = new FlinkKafkaProducer010<>(topic, streamIdToSerializationSchema.get(outputStreamId), nodeIdToProperties.get(FlinkExperimentFramework.nodeId));
				querySinkFunctions.add(p);
				ds.addSink(p);
			}*/
		}
		queries = outputStreamIdToUpdateQueries.getOrDefault(outputStreamId, new ArrayList<>());
		for (Map<String, Object> q : queries) {
			String sql_query = ((Map<String, String>)q.get("sql-query")).get("flink");
			tableEnv.sqlUpdate(sql_query);
		}
	}

	@Override
	public String AddQueries(Map<String, Object> query) {
		int outputStreamId = (int) query.get("output-stream-id");
		printDataStream.put(outputStreamId, (boolean) query.getOrDefault("print", false));
		String type_query = (String) query.get("type");
		tf.traceEvent(221, new Object[]{query.get("id")});
		if (type_query.equals("fetch-query")) {
			if (!outputStreamIdToFetchQueries.containsKey(outputStreamId)) {
				outputStreamIdToFetchQueries.put(outputStreamId, new ArrayList<>());
			}
			outputStreamIdToFetchQueries.get(outputStreamId).add(query);
		} else if (type_query.equals("update-query")) {
			if (!outputStreamIdToUpdateQueries.containsKey(outputStreamId)) {
				outputStreamIdToUpdateQueries.put(outputStreamId, new ArrayList<>());
			}
			outputStreamIdToUpdateQueries.get(outputStreamId).add(query);
		} else {
			throw new RuntimeException("Invalid query type specified");
		}
		return "Success";
	}

	@Override
	public String RunEnvironment() {
		if (interrupted) {
			System.out.println("Still waiting for the runtime environment to interrupt!");
			return "Error, runtime environment has not exited from its previous execution";
		}

		if (threadRunningEnvironment != null && threadRunningEnvironment.isAlive()) {
			//throw new RuntimeException("The execution environment is already running. " +
			//	                   	   "Stop it with stopEnvironment before running it again.");
			//System.out.println("The execution environment is already running. " +
			//	"Stop it with stopEnvironment before running it again.");
			return "Environment already running";
		}
		Set<Integer> streamIds = new HashSet<>();
		streamIds.addAll(outputStreamIdToFetchQueries.keySet());
		streamIds.addAll(outputStreamIdToUpdateQueries.keySet());
		for (int outputStreamId : streamIds) {
			DeployQueries(outputStreamId);
		}
		//deployedQueries = 0;
		//outputStreamIdToFetchQueries.clear();
		//outputStreamIdToUpdateQueries.clear();
		threadRunningEnvironment = new Thread(() -> {
			//System.out.println("Starting environment");
			Kafka09Fetcher.timeLastRecvdTuple = 0;
			try {
				env.execute();
			} catch (Exception e) {
				if (!interrupted) {
					// This interrupt was not because of us
					e.printStackTrace();
				}
				System.out.println("Stopping the execution environment");
			}
			Kafka09Fetcher.timeLastRecvdTuple = 0;
		});
		interrupted = false;
		threadRunningEnvironment.start();
		return "Success";
	}

	boolean interrupted = false;
	static int cnt3 = 0;
	@Override
	public String StopEnvironment() {
		System.out.println("Before tf.traceEvent(101);");
		tf.traceEvent(101);
		System.out.println("Before threadRunningEnvironment.interrupt();");
		threadRunningEnvironment.interrupt();
		System.out.println("Before threadRunningEnvironment.join();");
		threadRunningEnvironment = null;

		System.out.println("Before for (int stream_id : streamIdToDataStream.keySet())");
		for (int stream_id : streamIdToDataStream.keySet()) {
			DataStream<Row> ds = streamIdToDataStream.get(stream_id);
			System.out.println("ds.addSink(regularSinkFunctions.get(stream_id));");
			ds.addSink(regularSinkFunctions.get(stream_id));
		}

		System.out.println("Before tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());");
		tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
		System.out.println("Before return \"Success\"");
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "Success";
	}

	@Override
	public String AddDataset(Map<String, Object> ds) {
		String datasetType = (String) ds.get("type");
		int stream_id = (int) ds.get("stream-id");

		if (datasetType.equals("csv")) {
			/*String file_name = (String) ds.get("file");
			RowTypeInfo typeInfo = streamIdToTypeInfo.get(stream_id);
			TupleCsvInputFormat<Row> inputFormat = new RowCsvInputFormat(new Path(file_name), typeInfo);
			DataSet<Row> csv = new DataSource<>(execenv, inputFormat, typeInfo, Utils.getCallLocationName());
			try {
				ArrayList<Row> tuples = (ArrayList<Row>) csv.collect();
				allPackets = tuples;
				streamToTuples.put(stream_id, tuples);
				csv.print();
			} catch (Exception e) {
				e.printStackTrace();
			}*/
		} else if (ds.get("type").equals("yaml")) {
			Map<String, Object> map = GetMapFromYaml(ds);
			ArrayList<Map<String, Object> > tuples = (ArrayList<Map<String, Object> >) map.get("cepevents");
			for (Map<String, Object> tuple : tuples) {
				AddTuples(tuple, 1);
			}
		} else {
			throw new RuntimeException("Invalid dataset type for dataset with Id " + ds.get("id"));
		}
		return "Success";
	}

	private Map<String, Object> GetMapFromYaml(Map<String, Object> ds) {
		FileInputStream fis = null;
		Yaml yaml = new Yaml();
		try {
			fis = new FileInputStream((String) ds.get("file"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return yaml.load(fis);
	}

	int tupleCnt = 0;
	@Override
	public String ProcessTuples(int number_tuples) {
		System.out.println("Processing " + number_tuples);
		for (int stream_id : streamToTuples.keySet()) {
			TypeInformationSerializationSchema<Row> serializationSchema = streamIdToSerializationSchema.get(stream_id);
			String stream_name = (String) allSchemas.get(stream_id).get("name");
			for (int otherNodeId : streamIdToNodeIds.getOrDefault(stream_id, new ArrayList<>())) {
				String topicName = stream_name + "-" + otherNodeId;
				for (Row tuple : streamToTuples.get(stream_id)) {
					if (++tupleCnt % 100000 == 0) {
						System.out.println( System.nanoTime() + ": Sending tuple " + (++tupleCnt) + " to node " + otherNodeId + " with topic " + topicName);
					}
					this.nodeIdToKafkaProducer.get(otherNodeId).send(new ProducerRecord<>(topicName, serializationSchema.serialize(tuple)));
				}
			}
		}
		streamToTuples.clear();
		pktsPublished = 0;
		return "Success";
	}

	@Override
	public String ClearQueries() {
		tf.traceEvent(222);
		queries.clear();
		return "Success";
	}

	@Override
	public String ClearTuples() {
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
			long cur_time = System.currentTimeMillis();
			time_diff = cur_time - Kafka09Fetcher.timeLastRecvdTuple;
			System.out.println("NotifyAfterNoReceivedTuple, time_diff: " + time_diff + ", cur-time: " + cur_time + ", timeLastRecvdTuple: " + Kafka09Fetcher.timeLastRecvdTuple);
		} while (time_diff < milliseconds || Kafka09Fetcher.timeLastRecvdTuple == 0);
		return Long.toString(time_diff);
	}

	@Override
	public String TraceTuple(int tracepointId, List<String> arguments) {
		System.out.println("TraceTuple, tracepointId: " + tracepointId + ", arguments: " + arguments);
		tf.traceEvent(tracepointId, arguments.toArray());
		System.out.println("After tracing");
		return "Success";
	}

	public String Configure() {
		for (Map<String, Object> schema : allSchemas.values()) {
			AddKafkaConsumer(schema);
		}
		return "Success";
	}

	public static void main(String[] args) {
		boolean continue_running = true;
		while (continue_running) {
			FlinkExperimentFramework experimentAPI = new FlinkExperimentFramework();
			SpeComm speComm = new SpeComm(args, experimentAPI);
			experimentAPI.setNodeId(speComm.GetNodeId());
			experimentAPI.SetTraceOutputFolder(speComm.GetTraceOutputFolder());
			experimentAPI.setNodeId(speComm.GetNodeId());
			speComm.AcceptTasks();
		}
	}
}
