package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class WorkBalancerTest {

    private static int CONSUMERS_PER_SUBSCRIPTION = 2;
    private static int MAX_SUBSCRIPTIONS_PER_CONSUMER = 2;

    private WorkBalancer workBalancer = new WorkBalancer(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER);

    @Test
    public void shouldPerformSubscriptionsCleanup() {
        // given
        SubscriptionName s = anySubscription();
        SubscriptionAssignmentView currentState = new SubscriptionAssignmentView(ImmutableMap.of(s, ImmutableSet.of(assignment(s, "c1"))));

        // when
        SubscriptionAssignmentView target = workBalancer.balance(Collections.emptyList(), Collections.emptyList(), currentState);

        // then
        assertThat(target.getSubscriptions()).isEmpty();
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
        assertThat(view.getAssignmentsForSubscription(s)).extracting(SubscriptionAssignment::getSupervisorId).containsOnly("c1");
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
        assertThat(view.getAssignmentsForSubscription(subscription)).extracting(SubscriptionAssignment::getSupervisorId).containsOnly("c1");
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndSingleSubscription() {
        // given
        ImmutableList<String> supervisors = ImmutableList.of("c1", "c2");
        SubscriptionName subscription = anySubscription();
        ImmutableList<SubscriptionName> subscriptions = ImmutableList.of(subscription);

        // when
        SubscriptionAssignmentView view = workBalancer.balance(subscriptions, supervisors, new SubscriptionAssignmentView(emptyMap()));

        // then
        assertThat(view.getAssignmentsForSubscription(subscription)).extracting(SubscriptionAssignment::getSupervisorId).containsOnly("c1", "c2");
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndMultipleSubscriptions() {
        // given
        ImmutableList<String> supervisors = ImmutableList.of("c1", "c2");
        SubscriptionName s1 = anySubscription();
        SubscriptionName s2 = anySubscription();
        ImmutableList<SubscriptionName> subscriptions = ImmutableList.of(s1, s2);

        // when
        SubscriptionAssignmentView view = workBalancer.balance(subscriptions, supervisors, new SubscriptionAssignmentView(emptyMap()));

        // then
        assertThat(view.getAssignmentsForSubscription(s1)).extracting(SubscriptionAssignment::getSupervisorId).containsOnly("c1", "c2");
        assertThat(view.getAssignmentsForSubscription(s2)).extracting(SubscriptionAssignment::getSupervisorId).containsOnly("c1", "c2");
    }

    @Test
    public void shouldNotOverloadConsumers() {
        // given
        ImmutableList<String> supervisors = ImmutableList.of("c1");
        SubscriptionName s1 = anySubscription();
        SubscriptionName s2 = anySubscription();
        SubscriptionName s3 = anySubscription();
        ImmutableList<SubscriptionName> subscriptions = ImmutableList.of(s1, s2, s3);

        /* with a maximum of 2 subscriptions per consumer */

        // when
        SubscriptionAssignmentView view = workBalancer.balance(subscriptions, supervisors, new SubscriptionAssignmentView(emptyMap()));

        // then
        assertThat(view.getAssignmentsForSupervisor("c1")).hasSize(2);
    }

    @Test
    public void shouldRebalanceAfterConsumerDisappearing() {
        // given
        ImmutableList<String> supervisors = ImmutableList.of("c1", "c2");
        SubscriptionName s1 = anySubscription();
        SubscriptionName s2 = anySubscription();
        ImmutableList<SubscriptionName> subscriptions = ImmutableList.of(s1, s2);
        SubscriptionAssignmentView currentState = workBalancer.balance(subscriptions, supervisors, new SubscriptionAssignmentView(emptyMap()));

        // when
        ImmutableList<String> extendedSupervisorsList = ImmutableList.of("c1", "c3");
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(subscriptions, extendedSupervisorsList, currentState);

        // then
        assertThat(stateAfterRebalance.getSubscriptionsForSupervisor("c3")).containsOnly(s1, s2);
    }

    private SubscriptionAssignment assignment(SubscriptionName s1, String supervisorId) {
        return new SubscriptionAssignment(supervisorId, s1);
    }

    private SubscriptionName anySubscription() {
        return SubscriptionName.fromString("tech.topic$s" + UUID.randomUUID().getMostSignificantBits());
    }
}
