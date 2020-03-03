//
// Created by espen on 12.12.2019.
//

#ifndef T_REX_EXPERIMENTAPI_H
#define T_REX_EXPERIMENTAPI_H

#include "../external.hpp"
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
#include "yaml-cpp/yaml.h"
#include "../../../TRex2-lib/src/Common/tcpclient.h"

class ExperimentAPI {
public:
    ExperimentAPI() {
      packetQueueMutex = new pthread_mutex_t;
      pthread_mutex_init(packetQueueMutex, nullptr);
    }

    bool *cont;
    std::map<int, TcpClient*> clients;
    std::map<int, std::vector<int>> streamIdToNodes;

    std::string coordinator_ip = "";
    std::string coordinator_port = "";
    std::string client_ip = "";
    std::string client_port = "";

    int node_id;

    int interval_wait = 0;
    int batch_size = 1;
    std::map<int, YAML::Node> stream_id_to_schema;
    YAML::Node yaml_configuration;
    int totalPkts = 0;
    std::string trace_filename;
    bool continue_publishing = true;
    concept::packet::BufferedPacketUnmarshaller unmarshaller;
    concept::packet::PacketMarshaller marshaller;
    std::map<long, YAML::Node> schemas;
    TRexEngine *this_engine;
    int experiment_id;

    bool vldb_tweak_for_experiment4 = false;

    long long pktsPublished = 0;
    std::vector<std::pair<std::size_t, std::vector<char>>> allPackets;
    std::vector<PubPkt*> allRawPackets;
    std::vector<int> eventIDs;
    std::queue<PubPkt*> packetQueue;
    std::map<int, std::vector<YAML::Node>> dataset_to_tuples;

    pthread_mutex_t *packetQueueMutex;
    int number_threads = boost::thread::hardware_concurrency();
    std::string trace_output_folder;

    std::string Configure();
    std::string SetTupleBatchSize(int size);
    std::string SetIntervalBetweenTuples(int interval);
    std::string AddTuples(const YAML::Node& tuple, int quantity);
    std::string AddDataset(const YAML::Node& ds);
    std::string ProcessDataset(const YAML::Node& ds);
    std::string AddStreamSchemas(YAML::Node stream_schema);
    std::string AddQueries(const YAML::Node& query);
    std::string AddSubscriberOfStream(int stream_id, int nodeId);
    std::string SetNodeIdToAddress(const YAML::Node& newNodeIdToIpAndPort);
    std::string ProcessTuples(int number_tuples);
    std::string ClearQueries();
    std::string ClearTuples();
    std::string RunEnvironment();
    std::string StopEnvironment();
    std::string NotifyAfterNoReceivedTuple(int milliseconds);
    std::string HandleEvent(YAML::Node yaml_event);
    std::string AddTracepointIds(std::vector<int> tracepointIds);
    std::string CleanupExperiment();
    std::string TraceTuple(int tracepointId, std::vector<std::string> traceArguments);

    int RunExperiment();
};


#endif //T_REX_EXPERIMENTAPI_H
