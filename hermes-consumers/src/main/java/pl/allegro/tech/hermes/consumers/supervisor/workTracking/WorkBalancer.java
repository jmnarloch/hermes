package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class WorkBalancer {

    private static final Logger logger = LoggerFactory.getLogger(WorkBalancer.class);

    private final int consumersPerSubscription;
    private final int maxSubscriptionsPerConsumer;

    public WorkBalancer(int consumersPerSubscription, int maxSubscriptionsPerConsumer) {
        this.consumersPerSubscription = consumersPerSubscription;
        this.maxSubscriptionsPerConsumer = maxSubscriptionsPerConsumer;
    }

    public SubscriptionAssignmentView balance(List<SubscriptionName> subscriptions,
                                              List<String> supervisors,
                                              SubscriptionAssignmentView currentState) {

        SubscriptionAssignmentView state = SubscriptionAssignmentView.copyOf(currentState);

        removeInvalidSubscriptions(state, subscriptions);
        removeInvalidSupervisors(state, supervisors);

        addNewSubscriptions(state, subscriptions);
        addNewSupervisors(state, supervisors);

        do {
            assignSupervisors(state);
        } while (releaseWork(state));

        return state;
    }

    private boolean releaseWork(SubscriptionAssignmentView state) {
        int subscriptionsCount = state.getSubscriptions().size();
        int supervisorsCount = state.getSupervisors().size();
        int avgWork = subscriptionsCount * consumersPerSubscription / supervisorsCount;

        List<String> sortedSupervisors = state.getSupervisors().stream()
                .sorted((s1, s2) -> Integer.compare(state.getAssignmentsForSupervisor(s1).size(), state.getAssignmentsForSupervisor(s2).size()))
                .collect(toList());

        int median = supervisorsCount % 2 == 0
                ? (supervisorLoad(state, sortedSupervisors.get(supervisorsCount / 2)) + supervisorLoad(state, sortedSupervisors.get(supervisorsCount / 2 - 1))) / 2
                : supervisorLoad(state, sortedSupervisors.get(supervisorsCount / 2));

        String lowestLoadSupervisor = sortedSupervisors.get(0);
        int lowestLoad = supervisorLoad(state, lowestLoadSupervisor);
        if (lowestLoad < median && lowestLoad < avgWork) {
            state.getSubscriptions().stream()
                    .filter(s -> !state.getSubscriptionsForSupervisor(lowestLoadSupervisor).contains(s))
                    .max((s1, s2) -> Integer.compare(state.getAssignmentsForSubscription(s1).size(), state.getAssignmentsForSubscription(s2).size()))
                    .ifPresent(s -> state.getSupervisorsForSubscription(s).stream()
                        .max((c1, c2) -> Integer.compare(supervisorLoad(state, c1), supervisorLoad(state, c2)))
                            .ifPresent(c -> state.removeAssignment(s, c)));
            return true;
        }
        return false;

    }

    private int supervisorLoad(SubscriptionAssignmentView state, String supervisorId) {
        return state.getAssignmentsForSupervisor(supervisorId).size();
    }

    private void removeInvalidSubscriptions(SubscriptionAssignmentView state, List<SubscriptionName> subscriptions) {
        state.getSubscriptions().stream().filter(s -> !subscriptions.contains(s)).forEach(state::removeSubscription);
    }

    private void removeInvalidSupervisors(SubscriptionAssignmentView state, List<String> supervisors) {
        state.getSupervisors().stream().filter(s -> !supervisors.contains(s)).forEach(state::removeSupervisor);
    }

    private void addNewSubscriptions(SubscriptionAssignmentView state, List<SubscriptionName> subscriptions) {
        subscriptions.stream().filter(subscription -> !state.getSubscriptions().contains(subscription)).forEach(state::addSubscription);
    }

    private void addNewSupervisors(SubscriptionAssignmentView state, List<String> supervisors) {
        supervisors.stream().filter(supervisor -> !state.getSupervisors().contains(supervisor)).forEach(state::addSupervisor);
    }

    private void assignSupervisors(SubscriptionAssignmentView state) {
        while (true) {
            Set<String> availableSupervisors = availableSupervisors(state);

            if (availableSupervisors.isEmpty()) {
                if (workAvailable(state)) {
                    logger.warn("no more consumers available to perform work");
                }
                break;
            }

            Optional<SubscriptionName> subscription = getNextSubscription(state, availableSupervisors);

            if (subscription.isPresent()) {
                Optional<String> supervisor = getNextSupervisor(state, availableSupervisors, subscription.get());

                if (supervisor.isPresent()) {
                    state.addAssignment(subscription.get(), supervisor.get());
                } else {
                    logger.warn("no consumer supervisor for subscription {} - this should not happen!?", subscription.get());
                    break;
                }
            } else break;
        }
    }

    private Optional<SubscriptionName> getNextSubscription(SubscriptionAssignmentView state, Set<String> availableSupervisors) {
        return state.getSubscriptions().stream()
                .filter(s -> state.getAssignmentsForSubscription(s).size() < consumersPerSubscription)
                .filter(s -> !Sets.difference(availableSupervisors, state.getSupervisorsForSubscription(s)).isEmpty())
                .min((s1, s2) -> Integer.compare(state.getAssignmentsForSubscription(s1).size(), state.getAssignmentsForSubscription(s2).size()));
    }

    private Optional<String> getNextSupervisor(SubscriptionAssignmentView state, Set<String> availableSupervisors, SubscriptionName subscriptionName) {
        return availableSupervisors.stream()
            .filter(s -> !state.getSubscriptionsForSupervisor(s).contains(subscriptionName))
            .min((s1, s2) -> Integer.compare(state.getAssignmentsForSupervisor(s1).size(), state.getAssignmentsForSupervisor(s2).size()));
    }

    private boolean workAvailable(SubscriptionAssignmentView state) {
        return state.getSubscriptions().stream()
                .filter(s -> state.getAssignmentsForSubscription(s).size() < consumersPerSubscription)
                .findAny().isPresent();
    }

    private Set<String> availableSupervisors(SubscriptionAssignmentView state) {
        return state.getSupervisors().stream()
                .filter(s -> state.getSubscriptionsForSupervisor(s).size() < maxSubscriptionsPerConsumer)
                .filter(s -> !Sets.difference(state.getSubscriptions(), state.getSubscriptionsForSupervisor(s)).isEmpty())
                .collect(toSet());
    }
}
