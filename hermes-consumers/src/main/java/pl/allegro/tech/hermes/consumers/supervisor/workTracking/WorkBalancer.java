package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

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

        Set<SubscriptionName> canDetachSupervisorsFrom = new HashSet<>(state.getSubscriptions());

        do {
            assignSupervisors(state);
        } while (releaseWork(state, canDetachSupervisorsFrom));

        return state;
    }

    private boolean releaseWork(SubscriptionAssignmentView state, Set<SubscriptionName> canDetachSupervisorsFrom) {
        if (canDetachSupervisorsFrom.isEmpty()) {
            return false;
        }
        int subscriptionsCount = state.getSubscriptions().size();
        int supervisorsCount = state.getSupervisors().size();
        int avgWork = subscriptionsCount * consumersPerSubscription / supervisorsCount;

        List<String> sortedSupervisors = state.getSupervisors().stream()
                .sorted((s1, s2) -> Integer.compare(supervisorLoad(state, s1), supervisorLoad(state, s2)))
                .collect(toList());

        int median = supervisorsCount % 2 == 0
                ? (supervisorLoad(state, sortedSupervisors.get(supervisorsCount / 2)) + supervisorLoad(state, sortedSupervisors.get(supervisorsCount / 2 - 1))) / 2
                : supervisorLoad(state, sortedSupervisors.get(supervisorsCount / 2));

        String lowestLoadSupervisor = sortedSupervisors.get(0);
        int lowestLoad = supervisorLoad(state, lowestLoadSupervisor);
        if (lowestLoad < median && lowestLoad < avgWork) {

            Optional<String> maxLoadedSupervisor = state.getSupervisors().stream()
                    .filter(s -> !s.equals(lowestLoadSupervisor))
                    .filter(s -> Sets.difference(state.getSubscriptionsForSupervisor(s), canDetachSupervisorsFrom).isEmpty())
                    .max((s1, s2) -> Integer.compare(supervisorLoad(state, s1), supervisorLoad(state, s2)));

            if (maxLoadedSupervisor.isPresent()) {
                Optional<SubscriptionName> maxConsumedSubscription = state.getSubscriptionsForSupervisor(maxLoadedSupervisor.get()).stream()
                        .filter(canDetachSupervisorsFrom::contains)
                        .max((s1, s2) -> Integer.compare(assignmentsCount(state, s1), assignmentsCount(state, s2)));

                if (maxConsumedSubscription.isPresent()) {
                    state.removeAssignment(maxConsumedSubscription.get(), maxLoadedSupervisor.get());
                    canDetachSupervisorsFrom.remove(maxConsumedSubscription.get());
                    return true;
                }
            }
        }
        return false;

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
        try {
            Stream.generate(new AvailableWorkSupplier(state, consumersPerSubscription, maxSubscriptionsPerConsumer)).forEach(state::addAssignment);
        } catch (AvailableWorkSupplier.NoWorkAvailable ex) {
            if (workAvailable(state)) {
                logger.warn("Could not assign available work", ex);
            }
        }
    }

    private boolean workAvailable(SubscriptionAssignmentView state) {
        return state.getSubscriptions().stream()
                .filter(s -> assignmentsCount(state, s) < consumersPerSubscription)
                .findAny().isPresent();
    }

    private int assignmentsCount(SubscriptionAssignmentView state, SubscriptionName subscription) {
        return state.getAssignmentsForSubscription(subscription).size();
    }

    private int supervisorLoad(SubscriptionAssignmentView state, String supervisorId) {
        return state.getAssignmentsForSupervisor(supervisorId).size();
    }
}
