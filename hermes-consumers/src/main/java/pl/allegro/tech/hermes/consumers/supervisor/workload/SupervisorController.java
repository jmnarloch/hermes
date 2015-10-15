package pl.allegro.tech.hermes.consumers.supervisor.workload;

import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionCallback;

public interface SupervisorController extends SubscriptionCallback, SubscriptionAssignmentAware {
    void start() throws Exception;
    void shutdown() throws InterruptedException;
}
