package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class WorkTrackerTest extends ZookeeperBaseTest {
    static String basePath = "/consumers/runtime";
    static String supervisorId = "c1";
    static ExecutorService executorService = Executors.newSingleThreadExecutor();
    static SubscriptionRepository subscriptionRepository = mock(SubscriptionRepository.class);

    static WorkTracker workTracker = new WorkTracker(zookeeperClient, new ObjectMapper(), basePath, supervisorId,
            executorService, subscriptionRepository);

    @BeforeClass
    public static void before() throws Exception {
        workTracker.start(new ArrayList<>());
    }

    @AfterClass
    public static void after() throws IOException {
        workTracker.stop();
    }

    @Test
    public void shouldForceAssignment() {
        // given
        Subscription sub = anySubscription();

        // when
        forceAssignment(sub);

        // then
        assertThatCollectionContainsSupervisorId(workTracker.getAssignments(sub), supervisorId);
    }

    @Test
    public void shouldDropAssignment() {
        // given
        Subscription sub = forceAssignment(anySubscription());

        // when
        workTracker.dropAssignment(sub);
        wait.untilZookeeperPathIsEmpty(basePath + "/" + sub.toSubscriptionName());

        // then
        assertThat(workTracker.getAssignments(sub)).extracting(SubscriptionAssignment::getSupervisorId).doesNotContain(supervisorId);
    }

    @Test
    public void shouldReturnAllAssignments() {
        // given
        Subscription s1 = forceAssignment(anySubscription());
        Subscription s2 = forceAssignment(anySubscription());

        // when
        SubscriptionAssignmentView assignments = workTracker.getAssignments();

        // then
        assertThat(assignments.getSubscriptions()).containsOnly(s1.toSubscriptionName(), s2.toSubscriptionName());
        assertThatCollectionContainsSupervisorId(assignments.getAssignmentsForSubscription(s1.toSubscriptionName()), supervisorId);
        assertThatCollectionContainsSupervisorId(assignments.getAssignmentsForSubscription(s2.toSubscriptionName()), supervisorId);
    }

    @Test
    public void shouldApplyAssignmentChangesCreatingNewNodesInZookeeper() {
        // given
        Subscription s1 = anySubscription();
        SubscriptionAssignmentView view = new SubscriptionAssignmentView(ImmutableMap.of(s1.toSubscriptionName(), ImmutableSet.of(assignment(supervisorId, s1.toSubscriptionName()))));

        // when
        workTracker.apply(view);

        // then
        wait.untilZookeeperPathIsCreated(basePath + "/" + s1.toSubscriptionName() + "/" + supervisorId);
    }

    @Test
    public void shouldApplyAssignmentChangesByRemovingInvalidNodes() {
        // given
        Subscription s1 = forceAssignment(anySubscription());

        // when
        workTracker.apply(new SubscriptionAssignmentView(Collections.emptyMap()));

        // then
        wait.untilZookeeperPathNotExists(basePath + "/" + s1.toSubscriptionName() + "/" + supervisorId);
    }

    @Test
    public void shouldApplyAssignmentChangesByAddingNewNodes() {
        // given
        Subscription s1 = forceAssignment(anySubscription());
        Subscription s2 = forceAssignment(anySubscription());
        SubscriptionAssignmentView view = new SubscriptionAssignmentView(
                ImmutableMap.of(
                        s1.toSubscriptionName(), ImmutableSet.of(assignment(supervisorId, s1.toSubscriptionName()), assignment("otherConsumer", s1.toSubscriptionName())),
                        s2.toSubscriptionName(), ImmutableSet.of(assignment(supervisorId, s2.toSubscriptionName()))));

        // when
        workTracker.apply(view);

        // then
        wait.untilZookeeperPathIsCreated(basePath + "/" + s1.toSubscriptionName() + "/" + supervisorId);
        wait.untilZookeeperPathIsCreated(basePath + "/" + s1.toSubscriptionName() + "/" + "otherConsumer");
        wait.untilZookeeperPathIsCreated(basePath + "/" + s2.toSubscriptionName() + "/" + supervisorId);
    }

    private SubscriptionAssignment assignment(String supervisorId, SubscriptionName subscriptionName) {
        return new SubscriptionAssignment(supervisorId, subscriptionName);
    }

    private Subscription anySubscription() {
        SubscriptionName name = SubscriptionName.fromString("com.test.topic$" + Math.abs(UUID.randomUUID().getMostSignificantBits()));
        Subscription subscription = Subscription.fromSubscriptionName(name);
        given(subscriptionRepository.getSubscriptionDetails(name)).willReturn(subscription);
        return subscription;
    }

    private Subscription forceAssignment(Subscription sub) {
        workTracker.forceAssignment(sub);
        wait.untilZookeeperPathIsCreated(basePath + "/" + sub.toSubscriptionName() + "/" + supervisorId);
        return sub;
    }

    private void assertThatCollectionContainsSupervisorId(Collection<SubscriptionAssignment> assignments, String supervisorId) {
        assertThat(assignments).extracting(SubscriptionAssignment::getSupervisorId).contains(supervisorId);
    }

}
