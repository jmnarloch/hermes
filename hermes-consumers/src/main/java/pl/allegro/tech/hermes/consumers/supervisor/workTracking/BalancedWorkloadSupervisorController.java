package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;

import static java.util.Collections.emptyList;

public class BalancedWorkloadSupervisorController implements SupervisorController {
    private ConsumersSupervisor supervisor;
    private SubscriptionsCache subscriptionsCache;
    private WorkTracker workTracker;
    private ConsumersRegistry consumersRegistry;
    private ConfigFactory configFactory;

    private static final Logger logger = LoggerFactory.getLogger(BalancedWorkloadSupervisorController.class);

    public BalancedWorkloadSupervisorController(ConsumersSupervisor supervisor,
                                                SubscriptionsCache subscriptionsCache,
                                                WorkTracker workTracker,
                                                ConsumersRegistry consumersRegistry,
                                                ConfigFactory configFactory) {

        this.supervisor = supervisor;
        this.subscriptionsCache = subscriptionsCache;
        this.workTracker = workTracker;
        this.consumersRegistry = consumersRegistry;
        this.configFactory = configFactory;
    }

    @Override
    public void onSubscriptionAssigned(Subscription subscription) {
        logger.info("Assigning consumer for {}", subscription.getId());
        supervisor.assignConsumerForSubscription(subscription);
    }

    @Override
    public void onAssignmentRemoved(SubscriptionName subscription) {
        logger.info("Removing assignment from consumer for {}", subscription.getId());
        supervisor.deleteConsumerForSubscriptionName(subscription);
    }

    @Override
    public void start() throws Exception {
        subscriptionsCache.start(emptyList());
        workTracker.start(ImmutableList.of(this));
        supervisor.start();
        consumersRegistry.start();
        consumersRegistry.register(new BalanceWorkloadJob(
                consumersRegistry,
                subscriptionsCache,
                new WorkBalancer(2, 2),
                workTracker,
                configFactory.getIntProperty(Configs.CONSUMER_WORKLOAD_REBALANCE_INTERVAL)));
    }

    @Override
    public void shutdown() throws InterruptedException {
        supervisor.shutdown();
    }

    public String getId() {
        return consumersRegistry.getId();
    }

    public boolean isLeader() {
        return consumersRegistry.isLeader();
    }
}
