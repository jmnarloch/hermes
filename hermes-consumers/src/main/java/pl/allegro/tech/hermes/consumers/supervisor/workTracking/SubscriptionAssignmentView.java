package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.HashSet;
import java.util.Set;

public class SubscriptionAssignmentView {

    public Set<SubscriptionName> getSubscriptionSet() {
        return new HashSet<>();
    }
}
