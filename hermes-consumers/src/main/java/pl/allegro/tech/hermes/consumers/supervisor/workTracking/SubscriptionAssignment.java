package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Objects;

public class SubscriptionAssignment {
    private final String supervisorId;
    private final SubscriptionName subscriptionName;

    public SubscriptionAssignment(String supervisorId, SubscriptionName subscriptionName) {
        this.supervisorId = supervisorId;
        this.subscriptionName = subscriptionName;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public SubscriptionName getSubscriptionName() {
        return subscriptionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionAssignment that = (SubscriptionAssignment) o;
        return Objects.equals(supervisorId, that.supervisorId)
                && Objects.equals(subscriptionName, that.subscriptionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(supervisorId, subscriptionName);
    }
}