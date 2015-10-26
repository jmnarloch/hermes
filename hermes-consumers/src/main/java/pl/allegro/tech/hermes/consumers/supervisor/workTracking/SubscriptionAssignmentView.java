package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.*;

import static java.util.stream.Collectors.toSet;

public class SubscriptionAssignmentView {

    private Map<SubscriptionName, Set<SubscriptionAssignment>> subscriptionAssignments;
    private Map<String, Set<SubscriptionAssignment>> supervisorAssignments;

    public SubscriptionAssignmentView(Map<SubscriptionName, Set<SubscriptionAssignment>> view) {
        this.subscriptionAssignments = setupSubscriptionAssignments(view);
        this.supervisorAssignments = setupSupervisorAssignments(view);
    }

    private Map<SubscriptionName, Set<SubscriptionAssignment>> setupSubscriptionAssignments(Map<SubscriptionName, Set<SubscriptionAssignment>> view) {
        Map<SubscriptionName, Set<SubscriptionAssignment>> map = new HashMap<>();
        view.entrySet().stream().forEach(entry -> map.put(entry.getKey(), new HashSet<>(entry.getValue())));
        return map;
    }

    private Map<String, Set<SubscriptionAssignment>> setupSupervisorAssignments(Map<SubscriptionName, Set<SubscriptionAssignment>> view) {
        Map<String, Set<SubscriptionAssignment>> map = new HashMap<>();
        view.values().stream().flatMap(Set::stream).forEach(assignment -> {
            if (!map.containsKey(assignment.getSupervisorId())) {
                map.put(assignment.getSupervisorId(), new HashSet<>());
            }
            map.get(assignment.getSupervisorId()).add(assignment);
        });
        return map;
    }

    public Set<SubscriptionName> getSubscriptions() {
        return ImmutableSet.copyOf(subscriptionAssignments.keySet());
    }

    public Set<String> getSupervisors() {
        return ImmutableSet.copyOf(supervisorAssignments.keySet());
    }

    public Set<String> getSupervisorsForSubscription(SubscriptionName subscriptionName) {
        return getAssignmentsForSubscription(subscriptionName).stream().map(SubscriptionAssignment::getSupervisorId).collect(toSet());
    }

    public Set<SubscriptionAssignment> getAssignmentsForSubscription(SubscriptionName subscriptionName) {
        return Collections.unmodifiableSet(subscriptionAssignments.get(subscriptionName));
    }

    public Set<SubscriptionName> getSubscriptionsForSupervisor(String supervisorId) {
        return getAssignmentsForSupervisor(supervisorId).stream().map(SubscriptionAssignment::getSubscriptionName).collect(toSet());
    }

    public Set<SubscriptionAssignment> getAssignmentsForSupervisor(String supervisorId) {
        return Collections.unmodifiableSet(supervisorAssignments.get(supervisorId));
    }

    public void removeSubscription(SubscriptionName subscription) {
        supervisorAssignments.values().stream().forEach(assignments -> assignments.removeIf(assignment -> assignment.getSubscriptionName().equals(subscription)));
        subscriptionAssignments.remove(subscription);
    }

    public void removeSupervisor(String supervisorId) {
        subscriptionAssignments.values().stream().forEach(assignments -> assignments.removeIf(assignment -> assignment.getSupervisorId().equals(supervisorId)));
        supervisorAssignments.remove(supervisorId);
    }

    public void addSubscription(SubscriptionName subscriptionName) {
        subscriptionAssignments.putIfAbsent(subscriptionName, new HashSet<>());
    }

    public void addSupervisor(String supervisorId) {
        supervisorAssignments.putIfAbsent(supervisorId, new HashSet<>());
    }

    public void addAssignment(SubscriptionName subscriptionName, String supervisorId) {
        SubscriptionAssignment assignment = new SubscriptionAssignment(supervisorId, subscriptionName);
        subscriptionAssignments.get(subscriptionName).add(assignment);
        supervisorAssignments.get(supervisorId).add(assignment);
    }

    public void removeAssignment(SubscriptionName subscriptionName, String supervisorId) {
        SubscriptionAssignment assignment = new SubscriptionAssignment(supervisorId, subscriptionName);
        subscriptionAssignments.get(subscriptionName).remove(assignment);
        supervisorAssignments.get(supervisorId).remove(assignment);
    }

    public SubscriptionAssignmentView deletions(SubscriptionAssignmentView target) {
        return difference(this, target);
    }

    public SubscriptionAssignmentView additions(SubscriptionAssignmentView target) {
        return difference(target, this);
    }

    private static SubscriptionAssignmentView difference(SubscriptionAssignmentView first, SubscriptionAssignmentView second) {
        HashMap<SubscriptionName, Set<SubscriptionAssignment>> result = new HashMap<>();
        for (SubscriptionName subscription : first.getSubscriptions()) {
            Set<SubscriptionAssignment> assignments = first.getAssignmentsForSubscription(subscription);
            if (!second.getSubscriptions().contains(subscription)) {
                result.put(subscription, assignments);
            } else {
                Sets.SetView<SubscriptionAssignment> difference = Sets.difference(assignments, second.getAssignmentsForSubscription(subscription));
                if (!difference.isEmpty()) {
                    result.put(subscription, difference);
                }
            }
        }
        return new SubscriptionAssignmentView(result);
    }

    public static SubscriptionAssignmentView copyOf(SubscriptionAssignmentView currentState) {
        return new SubscriptionAssignmentView(currentState.subscriptionAssignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionAssignments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionAssignmentView that = (SubscriptionAssignmentView) o;
        return Objects.equals(subscriptionAssignments, that.subscriptionAssignments);
    }
}
