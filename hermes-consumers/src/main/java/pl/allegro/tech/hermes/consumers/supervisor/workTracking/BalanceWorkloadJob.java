package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class BalanceWorkloadJob implements LeaderLatchListener, Runnable {
    private ConsumersRegistry consumersRegistry;
    private SubscriptionsCache subscriptionsCache;
    private WorkBalancer workBalancer;
    private WorkTracker workTracker;
    private ScheduledExecutorService executorService;
    private int intervalSeconds;

    private ScheduledFuture job;

    public BalanceWorkloadJob(ConsumersRegistry consumersRegistry,
                              SubscriptionsCache subscriptionsCache,
                              WorkBalancer workBalancer,
                              WorkTracker workTracker,
                              int intervalSeconds) {
        this.consumersRegistry = consumersRegistry;
        this.subscriptionsCache = subscriptionsCache;
        this.workBalancer = workBalancer;
        this.workTracker = workTracker;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.intervalSeconds = intervalSeconds;
    }

    @Override
    public void run() {
        if (consumersRegistry.isLeader()) {
            workTracker.apply(workBalancer.balance(subscriptionsCache.listSubscriptionNames(),
                                                   consumersRegistry.list(),
                                                   workTracker.getAssignments()));
        }
    }

    @Override
    public void isLeader() {
        job = executorService.scheduleAtFixedRate(this, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void notLeader() {
        job.cancel(false);
    }
}
