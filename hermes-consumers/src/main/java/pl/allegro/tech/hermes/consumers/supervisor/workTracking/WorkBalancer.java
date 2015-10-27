package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Comparator;
import java.util.List;
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

        logger.info("Initializing workload balance [subscriptions_count=%s, consumers_count=%s]."
                , subscriptions.size(), supervisors.size());

        SubscriptionAssignmentView state = SubscriptionAssignmentView.copyOf(currentState);

        removeInvalidSubscriptions(state, subscriptions);
        removeInvalidSupervisors(state, supervisors);

        addNewSubscriptions(state, subscriptions);
        addNewSupervisors(state, supervisors);

        do {
            assignSupervisors(state);
        } while (releaseWork(state));

        logger.info("Finished workload balance [subscriptions_count=%s, consumers_count=%s].",
                subscriptions.size(), supervisors.size());
        return state;
    }

    private boolean releaseWork(SubscriptionAssignmentView state) {
        List<String> sortedByLoad = getSupervisorsSortedAscendingByLoad(state);
        int targetAverage = state.getSubscriptionsCount() * consumersPerSubscription / state.getSupervisorsCount();
        int currentMedian = getSupervisorsLoadMedian(state, sortedByLoad);

        String lowestLoadSupervisor = sortedByLoad.get(0);
        int lowestLoad = supervisorLoad(state, lowestLoadSupervisor);

        logger.debug("releaseWork [target_average=%s, current_median=%s, lowest_load_consumer=%s, lowest_load=%s].",
                targetAverage, currentMedian, lowestLoadSupervisor, lowestLoad);

        if (lowestLoad < currentMedian && lowestLoad < targetAverage) {
            String maxLoadedSupervisor = sortedByLoad.get(sortedByLoad.size() - 1);
            SubscriptionAssignment assignment = state.getAssignmentsForSupervisor(maxLoadedSupervisor).iterator().next();
            state.removeAssignment(assignment);
            return true;
        }
        return false;
    }

    private List<String> getSupervisorsSortedAscendingByLoad(SubscriptionAssignmentView state) {
        return state.getSupervisors().stream()
                .sorted(Comparator.comparingInt(s -> supervisorLoad(state, s)))
                .collect(toList());
    }

    private int getSupervisorsLoadMedian(SubscriptionAssignmentView state, List<String> sorted) {
        int count = sorted.size();
        return count % 2 == 0
                ? (supervisorLoad(state, sorted.get(count / 2)) + supervisorLoad(state, sorted.get(count / 2 - 1))) / 2
                : supervisorLoad(state, sorted.get(count / 2));
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
        return state.getAssignmentsCountForSubscription(subscription);
    }

    private int supervisorLoad(SubscriptionAssignmentView state, String supervisorId) {
        return state.getAssignmentsCountForSupervisor(supervisorId);
    }
}
