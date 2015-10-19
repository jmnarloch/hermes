package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableMap;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Map;
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
        return view.get(subscriptionName);
    }
}
