package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MutableWorkloadView {

    private final Map<SubscriptionName, Set<SubscriptionAssignment>> state;

    public MutableWorkloadView(SubscriptionAssignmentView currentState) {
        this.state = new HashMap<>();
        currentState.getSubscriptionSet().stream()
                .forEach(subscription -> state.put(subscription, new HashSet<>(currentState.getAssignments(subscription))));
    }

    public SubscriptionAssignmentView asSubscriptionAssignmentView() {
        return new SubscriptionAssignmentView(state);
    }

    public void removeInvalidSubscriptions(List<SubscriptionName> subscriptions) {
        state.keySet().removeIf(subscription -> !subscriptions.contains(subscription));
    }

    public void removeInvalidSupervisors(List<String> supervisors) {
        state.values().stream().forEach(assignments -> assignments.removeIf(assignment -> !supervisors.contains(assignment.getSupervisorId())));
    }
}
