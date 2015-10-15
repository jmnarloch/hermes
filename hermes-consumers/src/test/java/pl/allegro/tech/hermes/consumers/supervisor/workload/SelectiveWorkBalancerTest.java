package pl.allegro.tech.hermes.consumers.supervisor.workload;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.supervisor.workload.selective.WorkBalancingResult;
import pl.allegro.tech.hermes.consumers.supervisor.workload.selective.SelectiveWorkBalancer;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class SelectiveWorkBalancerTest {

    private static int CONSUMERS_PER_SUBSCRIPTION = 2;
    private static int MAX_SUBSCRIPTIONS_PER_CONSUMER = 2;

    private SelectiveWorkBalancer workBalancer = new SelectiveWorkBalancer(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER);

    @Test
    public void shouldPerformSubscriptionsCleanup() {
        // given
        List<SubscriptionName> subscriptions = someSubscriptions(1);
        List<String> supervisors = someSupervisors(1);
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors);

        // when
        WorkBalancingResult target = workBalancer.balance(emptyList(), supervisors, currentState);

        // then
        assertThat(target.getAssignmentsView().getSubscriptions()).isEmpty();
        assertThat(target.getRemovedSubscriptionsCount()).isEqualTo(subscriptions.size());
    }

    @Test
    public void shouldPerformSupervisorsCleanup() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(1);
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors);

        // when
        supervisors.remove(1);
        WorkBalancingResult work = workBalancer.balance(subscriptions, supervisors, currentState);

        // then
        assertThat(work.getRemovedSupervisorsCount()).isEqualTo(1);
        assertThat(work.getAssignmentsView().getAssignmentsForSubscription(subscriptions.get(0)))
                .extracting(SubscriptionAssignment::getConsumerNodeId).containsOnly(supervisors.get(0));
    }

    @Test
    public void shouldBalanceWorkForSingleSubscription() {
        // given
        List<String> supervisors = someSupervisors(1);
        List<SubscriptionName> subscriptions = someSubscriptions(1);

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors);

        // then
        assertThat(view.getAssignmentsForSubscription(subscriptions.get(0)))
                .extracting(SubscriptionAssignment::getConsumerNodeId).containsOnly(supervisors.get(0));
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndSingleSubscription() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(1);

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors);

        // then
        assertThat(view.getAssignmentsForSubscription(subscriptions.get(0))).extracting(SubscriptionAssignment::getConsumerNodeId).containsOnly(supervisors.get(0), supervisors.get(1));
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndMultipleSubscriptions() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(2);

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors);

        // then
        assertThat(view.getAssignmentsForSubscription(subscriptions.get(0))).extracting(SubscriptionAssignment::getConsumerNodeId).containsOnly(supervisors.get(0), supervisors.get(1));
        assertThat(view.getAssignmentsForSubscription(subscriptions.get(1))).extracting(SubscriptionAssignment::getConsumerNodeId).containsOnly(supervisors.get(0), supervisors.get(1));
    }

    @Test
    public void shouldNotOverloadConsumers() {
        // given
        List<String> supervisors = someSupervisors(1);
        List<SubscriptionName> subscriptions = someSubscriptions(3);

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors);

        // then
        assertThat(view.getAssignmentsForConsumerNode(supervisors.get(0))).hasSize(2);
    }

    @Test
    public void shouldRebalanceAfterConsumerDisappearing() {
        // given
        List<String> supervisors = ImmutableList.of("c1", "c2");
        List<SubscriptionName> subscriptions = someSubscriptions(2);
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors);

        // when
        List<String> extendedSupervisorsList = ImmutableList.of("c1", "c3");
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(subscriptions, extendedSupervisorsList, currentState).getAssignmentsView();

        // then
        assertThat(stateAfterRebalance.getSubscriptionsForConsumerNode("c3")).containsOnly(subscriptions.get(0), subscriptions.get(1));
    }

    @Test
    public void shouldAssignWorkToNewConsumersByWorkStealing() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(2);
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors);

        // when
        supervisors.add("new-supervisor");
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(subscriptions, supervisors, currentState).getAssignmentsView();

        // then
        assertThat(stateAfterRebalance.getAssignmentsForConsumerNode("new-supervisor").size()).isGreaterThan(0);
        assertThat(stateAfterRebalance.getAssignmentsForSubscription(subscriptions.get(0))).hasSize(2);
        assertThat(stateAfterRebalance.getAssignmentsForSubscription(subscriptions.get(1))).hasSize(2);
    }

    @Test
    public void shouldEquallyAssignWorkToConsumers() {
        // given
        SelectiveWorkBalancer workBalancer = new SelectiveWorkBalancer(2, 200);
        List<String> supervisors = ImmutableList.of("c1", "c2");
        List<SubscriptionName> subscriptions = someSubscriptions(50);
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, workBalancer);

        // when
        List<String> extendedSupervisorsList = ImmutableList.of("c1", "c2", "c3");
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(subscriptions, extendedSupervisorsList, currentState).getAssignmentsView();

        // then
        assertThat(stateAfterRebalance.getAssignmentsForConsumerNode("c3")).hasSize(50 * 2 / 3);
    }

    @Test
    public void shouldReassignWorkToFreeConsumers() {
        // given
        SelectiveWorkBalancer workBalancer = new SelectiveWorkBalancer(1, 100);
        List<String> supervisors = ImmutableList.of("c1");
        List<SubscriptionName> subscriptions = someSubscriptions(10);
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, workBalancer);

        // when
        ImmutableList<String> extendedSupervisorsList = ImmutableList.of("c1", "c2", "c3", "c4", "c5");
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(subscriptions, extendedSupervisorsList, currentState).getAssignmentsView();

        // then
        assertThat(stateAfterRebalance.getAssignmentsForConsumerNode("c5")).hasSize(2);
    }

    private SubscriptionAssignmentView initialState(List<SubscriptionName> subscriptions, List<String> supervisors) {
        return initialState(subscriptions, supervisors, workBalancer);
    }

    private SubscriptionAssignmentView initialState(List<SubscriptionName> subscriptions, List<String> supervisors, SelectiveWorkBalancer workBalancer) {
        return workBalancer.balance(subscriptions, supervisors, new SubscriptionAssignmentView(emptyMap())).getAssignmentsView();
    }

    private List<SubscriptionName> someSubscriptions(int count) {
        return IntStream.range(0, count).mapToObj(i -> anySubscription()).collect(toList());
    }

    private List<String> someSupervisors(int count) {
        return IntStream.range(0, count).mapToObj(i -> "c" + i).collect(toList());
    }

    private SubscriptionName anySubscription() {
        return SubscriptionName.fromString("tech.topic$s" + UUID.randomUUID().getMostSignificantBits());
    }
}
