package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.Sets;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

public class AvailableWorkSupplier implements Supplier<SubscriptionAssignment> {
    private SubscriptionAssignmentView state;
    private final int consumersPerSubscription;
    private final int maxSubscriptionsPerConsumer;

    public AvailableWorkSupplier(SubscriptionAssignmentView state, int consumersPerSubscription, int maxSubscriptionsPerConsumer) {
        this.state = state;
        this.consumersPerSubscription = consumersPerSubscription;
        this.maxSubscriptionsPerConsumer = maxSubscriptionsPerConsumer;
    }

    @Override
    public SubscriptionAssignment get() {
        Set<String> availableSupervisors = availableSupervisors(state);
        if (!availableSupervisors.isEmpty()) {
            return getNextSubscription(state, availableSupervisors)
                    .map(subscription -> getNextSubscriptionAssignment(state, availableSupervisors, subscription))
                    .orElseThrow(NoWorkAvailable::new).orElseThrow(NoWorkAvailable::new);
        }
        throw new NoWorkAvailable("no more consumers available to perform work");
    }

    private Optional<SubscriptionName> getNextSubscription(SubscriptionAssignmentView state, Set<String> availableSupervisors) {
        return state.getSubscriptions().stream()
                .filter(s -> state.getAssignmentsCountForSubscription(s) < consumersPerSubscription)
                .filter(s -> !Sets.difference(availableSupervisors, state.getSupervisorsForSubscription(s)).isEmpty())
                .min(Comparator.comparingInt(s -> state.getAssignmentsCountForSubscription(s)));
    }

    private Optional<SubscriptionAssignment> getNextSubscriptionAssignment(SubscriptionAssignmentView state, Set<String> availableSupervisors, SubscriptionName subscriptionName) {
        return availableSupervisors.stream()
                .filter(s -> !state.getSubscriptionsForSupervisor(s).contains(subscriptionName))
                .min(Comparator.comparingInt(s -> state.getAssignmentsCountForSupervisor(s)))
                .map(s -> new SubscriptionAssignment(s, subscriptionName));
    }

    private Set<String> availableSupervisors(SubscriptionAssignmentView state) {
        return state.getSupervisors().stream()
                .filter(s -> state.getAssignmentsCountForSupervisor(s) < maxSubscriptionsPerConsumer)
                .filter(s -> !Sets.difference(state.getSubscriptions(), state.getSubscriptionsForSupervisor(s)).isEmpty())
                .collect(toSet());
    }

    static class NoWorkAvailable extends RuntimeException {
        public NoWorkAvailable() {
        }

        public NoWorkAvailable(String message) {
            super(message);
        }
    }
}
