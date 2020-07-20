package no.uio.ifi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TaskRunner {
    int cmd_number = 0;
    Map<Integer, ExperimentAPI> nodeIdsToExperimentAPIs;

    TaskRunner(Map<Integer, ExperimentAPI> nodeIdsToExperimentAPIs) {
        this.nodeIdsToExperimentAPIs = nodeIdsToExperimentAPIs;
    }

}
