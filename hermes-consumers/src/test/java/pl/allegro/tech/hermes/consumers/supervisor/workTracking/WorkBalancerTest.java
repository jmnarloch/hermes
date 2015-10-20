package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Collections;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class WorkBalancerTest {

    private WorkBalancer workBalancer = new WorkBalancer();

    @Test
    public void shouldPerformSubscriptionsCleanup() {
        // given
        SubscriptionName s = anySubscription();
        SubscriptionAssignmentView currentState = new SubscriptionAssignmentView(ImmutableMap.of(s, ImmutableSet.of(assignment(s, "c1"))));

        // when
        SubscriptionAssignmentView target = workBalancer.balance(Collections.emptyList(), Collections.emptyList(), currentState);

        // then
        assertThat(target.getSubscriptionSet()).isEmpty();
    }

    @Test
    public void shouldPerformSupervisorsCleanup() {
        // given
        ImmutableList<String> supervisors = ImmutableList.of("c1");
        SubscriptionName s = anySubscription();
        ImmutableList<SubscriptionName> subscriptions = ImmutableList.of(s);
        SubscriptionAssignmentView currentState = new SubscriptionAssignmentView(ImmutableMap.of(s, ImmutableSet.of(assignment(s, "c1"), assignment(s, "c2-alreadyDisconnected"))));

        // when
        SubscriptionAssignmentView view = workBalancer.balance(subscriptions, supervisors, currentState);

        // then
        assertThat(view.getAssignments(s)).extracting(SubscriptionAssignment::getSupervisorId).containsOnly("c1");
    }

    @Test
    public void shouldBalanceWorkForSingleSubscription() {
        // given
        ImmutableList<String> supervisors = ImmutableList.of("c1");
        SubscriptionName subscription = anySubscription();
        ImmutableList<SubscriptionName> subscriptions = ImmutableList.of(subscription);

        // when
        SubscriptionAssignmentView view = workBalancer.balance(subscriptions, supervisors, new SubscriptionAssignmentView(emptyMap()));

        // then
        assertThat(view.getAssignments(subscription)).extracting(SubscriptionAssignment::getSupervisorId).isEqualTo("c1");
    }

    private SubscriptionAssignment assignment(SubscriptionName s1, String supervisorId) {
        return new SubscriptionAssignment(supervisorId, s1);
    }

    private SubscriptionName anySubscription() {
        return SubscriptionName.fromString("tech.topic$s" + UUID.randomUUID().getMostSignificantBits());
    }
}
