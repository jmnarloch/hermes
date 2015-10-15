package pl.allegro.tech.hermes.consumers.supervisor.workload.selective;

import com.codahale.metrics.Timer;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkTracker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static pl.allegro.tech.hermes.common.metric.Timers.CONSUMER_WORKLOAD_REBALANCE_DURATION;

public class BalancingJob implements LeaderLatchListener, Runnable {
    private ConsumerNodesRegistry consumersRegistry;
    private SubscriptionsCache subscriptionsCache;
    private SelectiveWorkBalancer workBalancer;
    private WorkTracker workTracker;
    private HermesMetrics metrics;
    private ScheduledExecutorService executorService;
    private int intervalSeconds;

    private ScheduledFuture job;

    private static final Logger logger = LoggerFactory.getLogger(BalancingJob.class);

    public BalancingJob(ConsumerNodesRegistry consumersRegistry,
                        SubscriptionsCache subscriptionsCache,
                        SelectiveWorkBalancer workBalancer,
                        WorkTracker workTracker,
                        HermesMetrics metrics,
                        int intervalSeconds) {
        this.consumersRegistry = consumersRegistry;
        this.subscriptionsCache = subscriptionsCache;
        this.workBalancer = workBalancer;
        this.workTracker = workTracker;
        this.metrics = metrics;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.intervalSeconds = intervalSeconds;
    }

    @Override
    public void run() {
        if (consumersRegistry.isLeader()) {
            try (Timer.Context ctx = metrics.timer(CONSUMER_WORKLOAD_REBALANCE_DURATION).time()) {
                logger.info("Initializing workload balance.");
                WorkBalancingResult work = workBalancer.balance(subscriptionsCache.listSubscriptionNames(),
                        consumersRegistry.list(),
                        workTracker.getAssignments());
                logger.info("Finished workload balance {}", work.printStats());
                WorkTracker.WorkDistributionChanges changes = workTracker.apply(work.getAssignmentsView());
                metrics.registerOrUpdateConsumersWorkloadGauges(work.getMissingResources(), changes.getDeletedAssignmentsCount(), changes.getCreatedAssignmentsCount());
            }
        } else {
            metrics.unregisterConsumersWorkloadGauges();
        }
    }

    @Override
    public void isLeader() {
        job = executorService.scheduleAtFixedRate(this, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void notLeader() {
        job.cancel(false);
        metrics.unregisterConsumersWorkloadGauges();
    }
}
