package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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

        logger.info("Initializing workload balance [subscriptions_count={}, consumers_count={}]."
                , subscriptions.size(), supervisors.size());

        SubscriptionAssignmentView state = SubscriptionAssignmentView.copyOf(currentState);

        removeInvalidSubscriptions(state, subscriptions);
        removeInvalidSupervisors(state, supervisors);

        addNewSubscriptions(state, subscriptions);
        addNewSupervisors(state, supervisors);

        assignSupervisors(state);

        reassignWork(state);

        logger.info("Finished workload balance [subscriptions_count={}, consumers_count={}].",
                subscriptions.size(), supervisors.size());
        return state;
    }

    private void reassignWork(SubscriptionAssignmentView state) {
        if (state.getSubscriptionsCount() < 2) {
            return;
        }
        boolean transferred;
        do {
            transferred = false;
            String maxLoaded = maxLoadedSupervisor(state);
            String minLoaded = minLoadedSupervisor(state);
            int maxLoad = supervisorLoad(state, maxLoaded);
            int minLoad = supervisorLoad(state, minLoaded);

            while (maxLoad > minLoad + 1) {
                Optional<SubscriptionName> subscription = getSubscriptionForTransfer(state, maxLoaded, minLoaded);
                if (subscription.isPresent()) {
                    state.removeAssignment(new SubscriptionAssignment(maxLoaded, subscription.get()));
                    state.addAssignment(new SubscriptionAssignment(minLoaded, subscription.get()));
                    transferred = true;
                } else break;
                maxLoad--;
                minLoad++;
            }
        } while(transferred);
    }

    private String maxLoadedSupervisor(SubscriptionAssignmentView state) {
        return state.getSupervisors().stream()
                .max(Comparator.comparingInt(s -> supervisorLoad(state, s)))
                .get();
    }

    private String minLoadedSupervisor(SubscriptionAssignmentView state) {
        return state.getSupervisors().stream()
                .min(Comparator.comparingInt(s -> supervisorLoad(state, s)))
                .get();
    }

    private Optional<SubscriptionName> getSubscriptionForTransfer(SubscriptionAssignmentView state, String maxLoaded, String minLoaded) {
        return state.getSubscriptionsForSupervisor(maxLoaded).stream()
                .filter(s -> !state.getSupervisorsForSubscription(s).contains(minLoaded))
                .findAny();

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
