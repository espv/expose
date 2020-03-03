//
// Created by espen on 12.12.2019.
//

#include "../external.hpp"
#include "../../../TRex2-lib/src/Common/tcpclient.h"
#include "experimentapi.h"
#include "../server.hpp"
#include "../test.hpp"
#include "../../../TRex2-lib/src/Common/trace-framework.hpp"
#include "../../../TRex2-lib/src/Packets/PubPkt.h"
#include "../Packet/Pkt.hpp"
#include "../../../TRex2-lib/src/Engine/TRexEngine.h"
#include "../Connection/RequestHandler.hpp"
#include "../Packet/PacketMarshaller.hpp"
#include "../Packet/BufferedPacketUnmarshaller.hpp"
#include "../Test/RuleR0.hpp"

#include <iostream>
#include <sys/syscall.h>
#include <stdlib.h>
#include <iostream>
#include <memory>
#include <cstring>
#include <ctime>
#include <queue>
#include <thread>
#include <sys/time.h>
#include <sys/resource.h>

#include <chrono>
#include "yaml-cpp/yaml.h"
#include <boost/asio.hpp>
#include "boost/random.hpp"
#include <boost/thread.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/algorithm/string.hpp>
#include <random>
#include <algorithm>


using namespace std;

std::string
ExperimentAPI::Configure() {
  cout << "Configuring" << endl;
  return "Success";
}

std::string
ExperimentAPI::SetTupleBatchSize(int size) {
  cout << "Setting tuple batch size to " << size << endl;
  batch_size = size;
  return "Success";
}

std::string
ExperimentAPI::SetIntervalBetweenTuples(int iw) {
  cout << "Setting the interval between tuples to " << iw << endl;
  interval_wait = iw;
  return "Success";
}

std::string
ExperimentAPI::AddTuples(const YAML::Node& tuple, int quantity) {
  //cout << "Adding " << quantity << " number of tuple " << tuple["id"] << endl;
  auto stream_id = tuple["stream-id"].as<int>();
  //cout << "stream-id: " << stream_id << endl;
  auto attributes = tuple["attributes"];
  Attribute attr[attributes.size()];
  memset(attr, 0, sizeof(Attribute)*attributes.size());

  for (unsigned long i = 0; i < attributes.size(); i++) {
    auto attribute = attributes[i];
    auto attr_name = attribute["name"].as<string>();
    // Turn first character to lowercase because that's a requirement in TESLA's grammar
    attr_name[0] = std::tolower(attr_name[0]);
    strcpy(attr[i].name, attr_name.c_str());
    auto attr_value = attribute["value"];
    switch (attr_value.Type()) {
      case YAML::NodeType::Null: {
        break;
      } case YAML::NodeType::Scalar: {
        try {
          float value = attr_value.as<float>();
          attr[i].type = FLOAT;
          attr[i].floatVal = value;
        } catch (const YAML::BadConversion& e) {
          try {
            int value = attr_value.as<int>();
            attr[i].type = INT;
            attr[i].intVal = value;
          } catch (const YAML::BadConversion& e) {
            try {
              string value = attr_value.as<string>();
              attr[i].type = STRING;
              strcpy(attr[i].stringVal, value.c_str());
            } catch (const YAML::BadConversion& e) {}
          }
        }
        break;
      } case YAML::NodeType::Sequence: {
        break;
      } case YAML::NodeType::Map: {
        break;
      } case YAML::NodeType::Undefined: {
        break;
      }
    }
  }

  PubPkt *pubPkt;
  // Tweak to make every third tuple of type 16, 17 and 18, in order to match the sequence query
  if (vldb_tweak_for_experiment4) {
    static int picker = 0;
    ++picker;
    pubPkt = new PubPkt(stream_id + (picker%3), attr, attributes.size());
  } else {
    pubPkt = new PubPkt(stream_id, attr, attributes.size());
  }

  static int tuple_cnt = 0;
  int tuple_id = tuple_cnt++;
  for (int i = 0; i < quantity; i++) {
    traceEvent(220, {tuple_id});
    auto marshalled_packet = marshaller.marshal(concept::packet::PktPtr(pubPkt));
    auto *marshalled_regular_packet = new vector<char>();
    for (char j : *marshalled_packet) {
      marshalled_regular_packet->push_back(j);
    }

    allRawPackets.emplace_back(pubPkt);
    allPackets.emplace_back(marshalled_regular_packet->size(), *marshalled_regular_packet);
    eventIDs.push_back(tuple_id);
  }

  return "Success";
}

std::string
ExperimentAPI::AddDataset(const YAML::Node& ds) {
  cout << "Adding dataset " << ds["id"] << endl;
  ifstream ifile(ds["file"].as<string>());
  auto dataset_type = ds["type"].as<string>();
  if (dataset_type == "csv") {
    vector<string> lines;
    vector<vector<string> > csv;
    string line;
    getline(ifile, line);

    stringstream lineStream(line);
    string cell;
    while (getline(ifile, cell, '\n')) {
      lines.push_back(cell);
    }
    // This checks for a trailing comma with no data after it.
    if (!lineStream && cell.empty()) { // If there was a trailing comma then add an empty element.
      lines.emplace_back("");
    }
    for (auto l : lines) {
      vector<string> new_row;
      boost::split(new_row, l, boost::is_any_of(","));
      csv.emplace_back(new_row);
    }

    auto stream_id = ds["stream-id"].as<long>();
    YAML::Node schema = stream_id_to_schema[stream_id];

    vector<pair<string, string> > attr_format;
    for (auto field : schema["tuple-format"].as<std::vector<YAML::Node>>()) {
      auto field_name = field["name"].as<string>();
      auto field_type = field["type"].as<string>();
      attr_format.emplace_back(pair<string, string>(field_name, field_type));
    }

    for (auto row : csv) {
      Attribute attr[row.size()];
      memset(attr, 0, sizeof(Attribute) * row.size());
      for (unsigned long i = 0; i < row.size(); i++) {
        string attr_name = attr_format.at(i).first;
        strcpy(attr[i].name, attr_name.c_str());
        auto attr_value = row.at(i);
        if (attr_format.at(i).second == "float" || attr_format.at(i).second == "double") {
          attr[i].type = FLOAT;
          attr[i].floatVal = atof(attr_value.c_str());
        } else if (attr_format.at(i).second == "int" || attr_format.at(i).second == "long" || attr_format.at(i).second == "long-timestamp") {
          attr[i].type = INT;
          attr[i].intVal = atol(attr_value.c_str());
        } else if (attr_format.at(i).second == "number") {
          attr[i].type = INT;
          attr[i].intVal = atoi(attr_value.c_str());
        } else if (attr_format.at(i).second == "bool") {
          attr[i].type = BOOL;
          attr[i].boolVal = atoi(attr_value.c_str());
        } else if (attr_format.at(i).second == "string" || attr_format.at(i).second == "timestamp") {
          attr[i].type = STRING;
          strcpy(attr[i].stringVal, ((string) attr_value).c_str());
        }  else {
          throw "Invalid attribute type in dataset";
        }
      }
      auto stream_id = ds["stream-id"].as<int>();
      auto pubPkt = new PubPkt(stream_id, attr, row.size());
      auto marshalled_packet = marshaller.marshal(concept::packet::PktPtr(pubPkt));
      auto *marshalled_regular_packet = new vector<char>();
      for (char j : *marshalled_packet) {
        marshalled_regular_packet->push_back(j);
      }

      allPackets.emplace_back(marshalled_regular_packet->size(), *marshalled_regular_packet);
      eventIDs.push_back(0);
    }
  } else if (dataset_type == "yaml") {
    YAML::Node dataset_tuples = YAML::LoadFile(ds["file"].as<string>());
    for (const YAML::Node& tuple : dataset_tuples["cepevents"].as<std::vector<YAML::Node>>()) {
      AddTuples(tuple, 1);
    }
  }
  return "Success";
}

std::string
ExperimentAPI::ProcessDataset(const YAML::Node& ds) {
  //cout << "Processing dataset " << ds["id"] << endl;
  ifstream ifile(ds["file"].as<string>());
  bool realistic_timing = false;

  int number_tuples = 0;
  auto dataset_type = ds["type"].as<string>();
  int dataset_id = ds["id"].as<int>();
  if (dataset_type == "csv") {
    vector<string> lines;
    vector<vector<string> > csv;
    string line;
    getline(ifile, line);

    stringstream lineStream(line);
    string cell;
    while (getline(ifile, cell, '\n')) {
      lines.push_back(cell);
    }
    // This checks for a trailing comma with no data after it.
    if (!lineStream && cell.empty()) { // If there was a trailing comma then add an empty element.
      lines.emplace_back("");
    }
    for (auto l : lines) {
      vector<string> new_row;
      boost::split(new_row, l, boost::is_any_of(","));
      csv.emplace_back(new_row);
    }

    auto stream_id = ds["stream-id"].as<long>();
    YAML::Node schema = stream_id_to_schema[stream_id];

    vector<pair<string, string> > attr_format;
    for (auto field : schema["tuple-format"].as<std::vector<YAML::Node>>()) {
      auto field_name = field["name"].as<string>();
      auto field_type = field["type"].as<string>();
      attr_format.emplace_back(pair<string, string>(field_name, field_type));
    }

    for (auto row : csv) {
      ++number_tuples;
      Attribute attr[row.size()];
      memset(attr, 0, sizeof(Attribute) * row.size());
      for (unsigned long i = 0; i < row.size(); i++) {
        string attr_name = attr_format.at(i).first;
        strcpy(attr[i].name, attr_name.c_str());
        auto attr_value = row.at(i);
        if (attr_format.at(i).second == "float" || attr_format.at(i).second == "double") {
          attr[i].type = FLOAT;
          attr[i].floatVal = atof(attr_value.c_str());
        } else if (attr_format.at(i).second == "int" || attr_format.at(i).second == "long" || attr_format.at(i).second == "long-timestamp") {
          attr[i].type = INT;
          attr[i].intVal = atol(attr_value.c_str());
        } else if (attr_format.at(i).second == "number") {
          attr[i].type = INT;
          attr[i].intVal = atoi(attr_value.c_str());
        } else if (attr_format.at(i).second == "bool") {
          attr[i].type = BOOL;
          attr[i].boolVal = atoi(attr_value.c_str());
        } else if (attr_format.at(i).second == "string" || attr_format.at(i).second == "timestamp") {
          attr[i].type = STRING;
          strcpy(attr[i].stringVal, ((string) attr_value).c_str());
        } else {
          throw "Invalid attribute type in dataset";
        }
      }
      auto stream_id = ds["stream-id"].as<int>();
      auto pubPkt = new PubPkt(stream_id, attr, row.size());
      auto marshalled_packet = marshaller.marshal(concept::packet::PktPtr(pubPkt));
      auto *marshalled_regular_packet = new vector<char>();
      for (char j : *marshalled_packet) {
        marshalled_regular_packet->push_back(j);
      }

      allPackets.emplace_back(marshalled_regular_packet->size(), *marshalled_regular_packet);
      eventIDs.push_back(0);
    }
  } else if (dataset_type == "yaml") {
    auto stream_id = ds["stream-id"].as<long>();
    YAML::Node schema = stream_id_to_schema[stream_id];
    double prevTimestamp = 0;
    auto rowtime_column = schema["rowtime-column"];
    auto prevTime = chrono::system_clock::now().time_since_epoch().count();
    if (dataset_to_tuples.find(dataset_id) == this->dataset_to_tuples.end()) {
      YAML::Node dataset_tuples = YAML::LoadFile(ds["file"].as<string>());
      dataset_to_tuples[dataset_id] = dataset_tuples["cepevents"].as<std::vector<YAML::Node>>();
    }
    std::vector<YAML::Node> cepevents = dataset_to_tuples[dataset_id];
    number_tuples = cepevents.size();
    for (YAML::Node tuple : cepevents) {
      AddTuples(tuple, 1);
      if (ds["realism"] && rowtime_column) {
        realistic_timing = true;
        auto time_column_name = rowtime_column["column"].as<std::string>();
        double timestamp = 0;
        for (auto attribute : tuple["attributes"].as<std::vector<YAML::Node>>()) {
          auto attribute_name = attribute["name"].as<std::string>();
          if (attribute_name == time_column_name) {
            auto nanoseconds_per_tick = rowtime_column["nanoseconds-per-tick"].as<double>();
            timestamp = attribute["value"].as<double>() * nanoseconds_per_tick;
            if (prevTimestamp == 0) {
              prevTimestamp = timestamp;
            }
            break;
          }
        }
        double time_diff_tuple = timestamp - prevTimestamp;
        auto time_diff_real = chrono::system_clock::now().time_since_epoch().count() - prevTime;
        while (time_diff_real < time_diff_tuple) {
          time_diff_real = chrono::system_clock::now().time_since_epoch().count() - prevTime;
        }

        prevTimestamp = timestamp;
        prevTime = chrono::system_clock::now().time_since_epoch().count();
        ProcessTuples(1);
      }
    }
  }
  if (!realistic_timing) {
    std::cout << "About to transmit tuples" << std::endl;
    ProcessTuples(number_tuples);
  }
  return "Success";
}

std::string
ExperimentAPI::AddStreamSchemas(YAML::Node stream_schemas) {
  cout << "Adding stream schema for streams ";
  for (YAML::iterator it = stream_schemas.begin(); it != stream_schemas.end(); ++it) {
    const YAML::Node& stream_schema = *it;
    cout << stream_schema["stream-id"] << " - " << stream_schema["name"] << ", ";
    stream_id_to_schema[stream_schema["stream-id"].as<int>()] = stream_schema;
  }
  cout << "so that they can be handled" << endl;
  return "Success";
}

std::string
ExperimentAPI::AddQueries(const YAML::Node& query) {
  cout << "Adding query " << query["id"] << endl;
  auto sql_query = query["sql-query"]["t-rex"].as<string>();
  if (std::all_of(sql_query.begin(),sql_query.end(),::isspace)) {
    std::cout << "Query is empty, nothing to add" << std::endl;
    return "Query is empty, nothing to add";
  }
  ofstream ofile("query_for_trex");
  std::cout << "Query: \n'" << sql_query << "'" << std::endl;
  ofile.write(sql_query.c_str(), sizeof(char)*sql_query.length());
  ofile.close();

  std::cout << "Running command `java -jar TRex-Java-client/TRex-client.jar localhost " + this->client_port + " -rule query_for_trex`" << std::endl;
  std::string cmd = "java -jar TRex-Java-client/TRex-client.jar localhost " + this->client_port + " -rule query_for_trex";
  system(cmd.c_str());
  return "Success";
}

std::string
ExperimentAPI::AddSubscriberOfStream(int stream_id, int node_id) {
  cout << "Adding Node " << node_id << " as subscriber of stream with Id " << stream_id << endl;
  // Need to find RequestHandler &parent of the node we want to add as subscriber.
  // We first get the IP and port from the slave, who gets it from the master,
  // and then we start a TCP connection via boost.
  this->streamIdToNodes[stream_id].emplace_back(node_id);
  this->this_engine->streamIdToNodes = this->streamIdToNodes;
  return "Success";
}

std::string
ExperimentAPI::SetNodeIdToAddress(const YAML::Node& newNodeIdToIpAndPort) {
  cout << "Updating Node Id and address" << endl;
  auto map = newNodeIdToIpAndPort.as<std::map<int, YAML::Node>>();

  for (auto const& [node_id, address] : map) {
    if (node_id == this->node_id) {
      continue;
    }
    std::cout << "Node " << node_id << " has IP " << address["ip"] << " and port " << address["port"] << std::endl;
    auto it = this->clients.find(node_id);
    if (it == this->clients.end()) {
      // Add new client
      for (int i = 0; i < number_threads; i++) {
        TcpClient *client = TcpClient::start_connection(address.as<std::map<std::string, std::string>>());
        client->set_node_id(node_id);
        new boost::thread(boost::bind(&boost::asio::io_service::run, client->io_service));
        this->this_engine->clients[node_id].push_back(client);
      }
    }
  }
  return "Success";
}

std::string
ExperimentAPI::ProcessTuples(int number_tuples) {
  // TODO: Transmit tuples to subscribers by using the clients map
  // I need to implement a send method in the TcpClient class that can send PubPkts
  cout << "Processing " << number_tuples << " tuples" << endl;
  //cout << "Processing packets" << endl;
  // Invoke all threads for this loop. Perhaps they have to wait until I wait a few milliseconds and unlock a mutex
  for (int i = 0; i < this->allRawPackets.size() && i < number_tuples; i++) {
    PubPkt *rawPkt = this->allRawPackets.at(i);
    std::pair<std::size_t, std::vector<char>> marshalled_pkt = this->allPackets.at(i);
    int tuple_stream_id = rawPkt->getEventType();
    if (vldb_tweak_for_experiment4) {
      // This tweak is to ensure that both 16, 17 and 18 get transmitted to Node 2, even though only 16 is supposed to
      tuple_stream_id = 16;
    }
    for (auto nid : this->streamIdToNodes[tuple_stream_id]) {
      auto *pair = new std::pair<std::size_t, std::vector<char>>(marshalled_pkt.first, marshalled_pkt.second);
      this->this_engine->clients[nid][rand() % number_threads]->sendPubPkt(pair);
    }
  }
  return "Success";
}

std::string
ExperimentAPI::ClearQueries() {
  cout << "Clearing queries" << endl;
  delete this_engine;
  this_engine = new TRexEngine(number_threads);
  this_engine->finalize();
  traceEvent(222);
  return "Success";
}

std::string
ExperimentAPI::ClearTuples() {
  cout << "Clearing tuples" << endl;
  allPackets.clear();
  traceEvent(223);
  return "Success";
}

std::string
ExperimentAPI::RunEnvironment() {
  cout << "Running environment" << endl;
  return "Success";
}

std::string
ExperimentAPI::StopEnvironment() {
  cout << "Stopping environment" << endl;
  writeBufferToFile();
  this->this_engine->deleteStacksRules();
  return "Success";
}

std::string
ExperimentAPI::CleanupExperiment() {
  cout << "Cleanup after experiment" << endl;
  writeBufferToFile();
  *cont = false;
  return "Success";
}

std::string
ExperimentAPI::AddTracepointIds(std::vector<int> tracepointIds) {
  setupTraceFramework(tracepointIds, this->trace_output_folder);
  return "Success";
}

std::string
ExperimentAPI::NotifyAfterNoReceivedTuple(int milliseconds) {
  long time_diff = 0;
  do {
    int microseconds = milliseconds * 1000;
    usleep(microseconds);
    time_diff = chrono::system_clock::now().time_since_epoch().count()/1000000 - this->this_engine->timeLastRecvdTuple;
    std::cout << "NotifyAfterNoReceivedTuple, time_diff: " << time_diff << ", milliseconds: " << milliseconds << std::endl;
  } while (time_diff < milliseconds || this->this_engine->timeLastRecvdTuple == 0);
  cout << "timeLastRecvdTuple: " << this->this_engine->timeLastRecvdTuple << std::endl;
  this->this_engine->timeLastRecvdTuple = 0;
  return std::to_string(time_diff);
}

std::string
ExperimentAPI::TraceTuple(int tracepointId, std::vector<std::string> traceArguments) {
  // The empty vector is a vector of ints, but we use string arguments here
  traceEvent(tracepointId, {}, traceArguments);
  return "Success";
}

std::string ExperimentAPI::HandleEvent(YAML::Node yaml_event) {
  auto cmd = yaml_event["task"].as<string>();
  YAML::Node args = yaml_event["arguments"];
  std::string ret;
  if (cmd == "Configure") {
    ret = this->Configure();
  } else if (cmd == "SetTupleBatchSize") {
    this->batch_size = args[0].as<int>();
    ret = this->SetTupleBatchSize(batch_size);
  } else if (cmd == "SetIntervalBetweenTuples") {
    int interval = args[0].as<int>();
    ret = this->SetIntervalBetweenTuples(interval);
  } else if (cmd == "AddTuples") {
    int quantity = args[0].as<int>();
    ret = this->AddTuples(args[0], quantity);
  } else if (cmd == "AddDataset") {
    ret = this->AddDataset(args[0]);
  } else if (cmd == "ProcessDataset") {
    ret = this->ProcessDataset(args[0]);
  } else if (cmd == "AddStreamSchemas") {
    ret = this->AddStreamSchemas(args);
  } else if (cmd == "AddQueries") {
    ret = this->AddQueries(args[0]);
  } else if (cmd == "AddSubscriberOfStream") {
    int stream_id = (int) args[0].as<int>();
    int nodeId = (int) args[1].as<int>();
    ret = this->AddSubscriberOfStream(stream_id, nodeId);
  } else if (cmd == "SetNodeIdToAddress") {
    ret = this->SetNodeIdToAddress(args[0]);
  } else if (cmd == "ProcessTuples") {
    int number_tuples = args[0].as<int>();
    ret = this->ProcessTuples(number_tuples);
  } else if (cmd == "ClearQueries") {
    ret = this->ClearQueries();
  } else if (cmd == "ClearTuples") {
    ret = this->ClearTuples();
  } else if (cmd == "RunEnvironment") {
    ret = this->RunEnvironment();
  } else if (cmd == "StopEnvironment") {
    ret = this->StopEnvironment();
  } else if (cmd == "CleanupExperiment") {
    ret = this->CleanupExperiment();
  } else if (cmd == "AddTracepointIds") {
    ret = this->AddTracepointIds(args.as<std::vector<int>>());
  } else if (cmd == "NotifyAfterNoReceivedTuple") {
    ret = this->NotifyAfterNoReceivedTuple(args[0].as<int>());
  } else if (cmd == "TraceTuple") {
    ret = this->TraceTuple(args[0].as<int>(), args[1].as<std::vector<std::string>>());
  } else {
    throw invalid_argument("Invalid task from slave: " + cmd);
  }

  return ret;
}

int ExperimentAPI::RunExperiment() {
  char *coordinator_ipStr = const_cast<char *>(this->coordinator_ip.c_str());
  char *coordinator_portNum = const_cast<char *>(coordinator_port.c_str());
  char client_ipStr[strlen(this->client_ip.c_str())+2];
  int i;
  for (i = 0; i < this->client_ip.size(); i++) {
    client_ipStr[i] = this->client_ip.c_str()[i];
  }
  client_ipStr[i++] = '\n';
  client_ipStr[i++] = '\0';
  char client_portNum[this->client_port.size()+2];
  for (i = 0; i < strlen(this->client_port.c_str()); i++) {
    client_portNum[i] = this->client_port.c_str()[i];
  }
  client_portNum[i++] = '\n';
  client_portNum[i++] = '\0';

  std::string node_id_str = std::to_string(this->node_id);
  char nodeIdNum[std::to_string(this->node_id).size()+2];
  for (i = 0; i < strlen(node_id_str.c_str()); i++) {
    nodeIdNum[i] = node_id_str.c_str()[i];
  }
  nodeIdNum[i++] = '\n';
  nodeIdNum[i++] = '\0';

  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  char buffer[1024] = {0};
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    printf("\n Socket creation error \n");
    return -1;
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(std::stoi(coordinator_portNum));

  // Convert IPv4 and IPv6 addresses from text to binary form
  if(inet_pton(AF_INET, coordinator_ipStr, &serv_addr.sin_addr)<=0)
  {
    printf("\nInvalid address/ Address not supported \n");
    return -1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
  {
    printf("\nConnection Failed \n");
    return -1;
  }
  write(sock, nodeIdNum, strlen(nodeIdNum));
  usleep(1000);
  write(sock, client_ipStr, strlen(client_ipStr));
  usleep(1000);
  write(sock, client_portNum, strlen(client_portNum));
  usleep(1000);
  while (*cont) {
    char line[1024*1024];
    char c = -1;
    int cnt = 0;
    while (true) {
      int r = read(sock, &c, 1);
      if (r < 0) {
        close(sock);
        int one = 1;
        setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(int));
        *cont = false;
        break;
      } else if (r == 0 || c == '\n') {
        line[cnt] = '\0';
        break;
      }
      line[cnt++] = c;
    }
    line[cnt] = '\0';
    if (cnt == 0) {
      *cont = false;
      continue;
    }
    std::cout << "Received cmd " << line << std::endl;
    for (int i = 0; line[i] != '\0'; i++) {
      if (line[i] == '\n') {
        line[i] = '\0';
        break;
      }
      if (line[i] == '\\' && line[i + 1] == 'n') {
        line[i] = '\n';
        for (int j = i + 1; line[j] != '\n' && line[j] != '\0'; j++) {
          line[j] = line[j + 1];
        }
      }
    }

    YAML::Node yaml_event = YAML::Load(line);
    std::string response = HandleEvent(yaml_event) + "\n";
    int sent = 0;
    while (sent < response.length()) {
      int s = write(sock, &response.c_str()[sent], response.length()-sent);
      if (s < 0) {
        close(sock);
        int one = 1;
        setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(int));
        *cont = false;
        break;
      }
      sent += s;
    }
  }

  close(sock);

  exit(0);
  return 0;
}
