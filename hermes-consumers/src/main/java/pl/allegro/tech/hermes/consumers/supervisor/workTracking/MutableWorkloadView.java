package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.*;

import static java.util.stream.Collectors.toList;

public class MutableWorkloadView {

    private static final Logger logger = LoggerFactory.getLogger(MutableWorkloadView.class);

    private final static int MINIMAL_CONSUMERS_PER_SUBSCRIPTION = 2;
    private final static int MAXIMUM_SUBSCRIPTIONS_PER_CONSUMER = 2;

    private final Map<SubscriptionName, Set<SubscriptionAssignment>> state;

    private final Map<String, Set<SubscriptionName>> assignments = new HashMap<>();

    public MutableWorkloadView(SubscriptionAssignmentView currentState) {
        this.state = new HashMap<>();
        currentState.getSubscriptionSet().stream()
                .forEach(subscription -> state.put(subscription, new HashSet<>(currentState.getAssignments(subscription))));

        setupAssignments();
    }

    private void setupAssignments() {
        state.values().stream().flatMap(Set::stream).forEach(this::addAssignment);
    }

    private void addAssignment(SubscriptionAssignment assignment) {
        String supervisor = assignment.getSupervisorId();
        SubscriptionName subscription = assignment.getSubscriptionName();
        if (assignments.containsKey(supervisor)) {
            assignments.get(supervisor).add(subscription);
        } else {
            assignments.put(supervisor, new HashSet<>(Arrays.asList(subscription)));
        }
    }

    public SubscriptionAssignmentView asSubscriptionAssignmentView() {
        return new SubscriptionAssignmentView(state);
    }

    public void removeInvalidSubscriptions(List<SubscriptionName> subscriptions) {
        state.keySet().removeIf(subscription -> !subscriptions.contains(subscription));
    }

    public void removeInvalidSupervisors(List<String> supervisors) {
        state.values().stream()
                .forEach(assignments -> assignments.removeIf(assignment -> !supervisors.contains(assignment.getSupervisorId())));
    }

    public void addNewSubscriptions(List<SubscriptionName> subscriptions) {
        subscriptions.stream()
                .filter(subscription -> !state.containsKey(subscription))
                .forEach(subscription -> state.put(subscription, new HashSet<>()));
    }

    public void addNewSupervisors(List<String> supervisors) {
        supervisors.stream()
                .filter(supervisor -> !assignments.containsKey(supervisor))
                .forEach(supervisor -> assignments.put(supervisor, new HashSet<>()));
    }

    public void assignSupervisors() {

        while (true) {
            List<String> availableSupervisors = availableSupervisors();

            if (availableSupervisors.isEmpty()) {
                if (workAvailable()) {
                    logger.warn("no more consumers available to perform work");
                }
                break;
            }

            Optional<SubscriptionName> subscription = getNextSubscription(availableSupervisors);

            if (subscription.isPresent()) {
                Optional<String> supervisor = getNextSupervisor(availableSupervisors, subscription.get());

                if (supervisor.isPresent()) {
                    state.get(subscription.get()).add(new SubscriptionAssignment(supervisor.get(), subscription.get()));
                    assignments.get(supervisor.get()).add(subscription.get());
                } else {
                    logger.warn("no consumer supervisor for subscription {} - this should not happen!?", subscription.get());
                    break;
                }
            } else {
                logger.warn("no consumer supervisor can be assigned to subscription {}", subscription.get());
                break;
            }
        }
    }

    private boolean workAvailable() {
        return state.values().stream().filter(assignments -> assignments.size() < MINIMAL_CONSUMERS_PER_SUBSCRIPTION).findAny().isPresent();
    }

    private Optional<String> getNextSupervisor(List<String> availableSupervisors, SubscriptionName subscription) {
        return availableSupervisors.stream()
                .filter(supervisor -> !assignments.get(supervisor).contains(subscription))
                .min((s1, s2) -> Integer.compare(assignments.get(s1).size(), assignments.get(s2).size()));
    }

    private List<String> availableSupervisors() {
        return assignments.entrySet().stream()
                .filter(entry -> !Sets.difference(state.keySet(), entry.getValue()).isEmpty()) // there are subscriptions that are not consumed by this consumer
                .filter(entry -> entry.getValue().size() < MAXIMUM_SUBSCRIPTIONS_PER_CONSUMER) // is able to consume
                .map(entry -> entry.getKey())
                .collect(toList());
    }

    private Optional<SubscriptionName> getNextSubscription(List<String> consumers) {
        return state.entrySet().stream()
                .filter(entry -> assignments.entrySet().stream()
                        .filter(e -> consumers.contains(e.getKey())) // consumer is able to consume
                        .filter(e -> !e.getValue().contains(entry.getKey())) // subscription can be consumed by one of the available consumers
                        .findAny().isPresent())
                .min((s1, s2) -> Integer.compare(s1.getValue().size(), s2.getValue().size()))
                .map(e -> e.getKey());
    }
}