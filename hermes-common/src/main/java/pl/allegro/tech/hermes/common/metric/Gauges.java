package pl.allegro.tech.hermes.common.metric;

import static pl.allegro.tech.hermes.metrics.PathsCompiler.GROUP;
import static pl.allegro.tech.hermes.metrics.PathsCompiler.HOSTNAME;
import static pl.allegro.tech.hermes.metrics.PathsCompiler.SUBSCRIPTION;
import static pl.allegro.tech.hermes.metrics.PathsCompiler.TOPIC;

public class Gauges {

    public static final String PRODUCER_EVERYONE_CONFIRMS_BUFFER_TOTAL_BYTES = "producer." + HOSTNAME + ".everyone-confirms-buffer-total-bytes",
            PRODUCER_EVERYONE_CONFIRMS_BUFFER_AVAILABLE_BYTES = "producer." + HOSTNAME + ".everyone-confirms-buffer-available-bytes",
            PRODUCER_EVERYONE_CONFIRMS_COMPRESSION_RATE = "producer." + HOSTNAME + ".everyone-confirms-compression-rate-avg",
            PRODUCER_LEADER_CONFIRMS_BUFFER_TOTAL_BYTES = "producer." + HOSTNAME + ".leader-confirms-buffer-total-bytes",
            PRODUCER_LEADER_CONFIRMS_BUFFER_AVAILABLE_BYTES = "producer." + HOSTNAME + ".leader-confirms-buffer-available-bytes",
            PRODUCER_LEADER_CONFIRMS_COMPRESSION_RATE = "producer." + HOSTNAME + ".leader-confirms-compression-rate-avg",
            PRODUCER_JMX_PREFIX = "producer." + HOSTNAME + ".jmx",

            CONSUMER_THREADS = "consumer." + HOSTNAME + ".threads",
            CONSUMER_OUTPUT_RATE = "consumer." + HOSTNAME + ".output-rate." + GROUP + "." + TOPIC + "." + SUBSCRIPTION,

            CONSUMERS_WORKLOAD_SELECTIVE_MISSING_RESOURCES = "consumers-workload.selective.missing-resources",
            CONSUMERS_WORKLOAD_SELECTIVE_DELETED_ASSIGNMENTS = "consumers-workload.selective.deleted-assignments",
            CONSUMERS_WORKLOAD_SELECTIVE_CREATED_ASSIGNMENTS = "consumers-workload.selective.created-assignments";
}