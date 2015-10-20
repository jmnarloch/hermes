package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SubscriptionAssignmentView {
    private Map<SubscriptionName, Set<SubscriptionAssignment>> view;

    public SubscriptionAssignmentView(Map<SubscriptionName, Set<SubscriptionAssignment>> view) {
        this.view = ImmutableMap.copyOf(view);
    }

    public Set<SubscriptionName> getSubscriptionSet() {
        return view.keySet();
    }

    public Set<SubscriptionAssignment> getAssignments(SubscriptionName subscriptionName) {
        return Collections.unmodifiableSet(view.get(subscriptionName));
    }

    public SubscriptionAssignmentView deletions(SubscriptionAssignmentView target) {
        return difference(this, target);
    }

    public SubscriptionAssignmentView additions(SubscriptionAssignmentView target) {
        return difference(target, this);
    }

    private static SubscriptionAssignmentView difference(SubscriptionAssignmentView first, SubscriptionAssignmentView second) {
        HashMap<SubscriptionName, Set<SubscriptionAssignment>> result = new HashMap<>();
        for (SubscriptionName subscription : first.getSubscriptionSet()) {
            Set<SubscriptionAssignment> assignments = first.getAssignments(subscription);
            if (!second.getSubscriptionSet().contains(subscription)) {
                result.put(subscription, assignments);
            } else {
                Sets.SetView<SubscriptionAssignment> difference = Sets.difference(assignments, second.getAssignments(subscription));
                if (!difference.isEmpty()) {
                    result.put(subscription, difference);
                }
            }
        }
        return new SubscriptionAssignmentView(result);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionAssignmentView that = (SubscriptionAssignmentView) o;
        return Objects.equals(view, that.view);
    }

    @Override
    public int hashCode() {
        return Objects.hash(view);
    }
}
