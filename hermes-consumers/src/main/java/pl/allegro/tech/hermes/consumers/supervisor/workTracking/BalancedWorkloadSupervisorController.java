package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableList;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;

public class BalancedWorkloadSupervisorController implements SupervisorController {
    private ConsumersSupervisor supervisor;
    private SubscriptionsCache subscriptionsCache;
    private WorkTracker workTracker;
    private ConsumersRegistry consumersRegistry;
    private String supervisorId;

    public BalancedWorkloadSupervisorController(ConsumersSupervisor supervisor,
                                                SubscriptionsCache subscriptionsCache,
                                                WorkTracker workTracker,
                                                ConsumersRegistry consumersRegistry,
                                                String supervisorId) {
        this.supervisor = supervisor;
        this.subscriptionsCache = subscriptionsCache;
        this.workTracker = workTracker;
        this.consumersRegistry = consumersRegistry;
        this.supervisorId = supervisorId;
    }

    @Override
    public void onSubscriptionCreated(Subscription subscription) {

    }

    @Override
    public void onSubscriptionRemoved(Subscription subscription) {

    }

    @Override
    public void onSubscriptionChanged(Subscription subscription) {

    }

    @Override
    public void onSubscriptionAssigned(Subscription subscription) {

    }

    @Override
    public void onAssignmentRemoved(SubscriptionName subscriptionName) {

    }

    @Override
    public void start() throws Exception {
        subscriptionsCache.start(ImmutableList.of(this));
        workTracker.start(ImmutableList.of(this));
        supervisor.start();
        consumersRegistry.register(supervisorId);
    }

    @Override
    public void shutdown() throws InterruptedException {
        supervisor.shutdown();
    }

    public String getId() {
        return supervisorId;
    }

    public boolean isLeader() {
        return false;
    }
}
