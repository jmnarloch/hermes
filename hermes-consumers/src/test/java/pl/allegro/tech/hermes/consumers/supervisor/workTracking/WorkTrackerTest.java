package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.io.IOException;
import java.util.UUID;
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
        workTracker.start();
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
        workTracker.forceAssignment(sub);
        wait.untilZookeeperPathIsCreated(basePath + "/" + sub.toSubscriptionName() + "/" + supervisorId );

        // then
        assertThat(workTracker.getAssignments(sub)).extracting(SubscriptionAssignment::getSupervisorId).contains(supervisorId);
    }

    private Subscription anySubscription() {
        SubscriptionName name = SubscriptionName.fromString("com.test.topic$" + Math.abs(UUID.randomUUID().getMostSignificantBits()));
        Subscription subscription = Subscription.fromSubscriptionName(name);
        given(subscriptionRepository.getSubscriptionDetails(name)).willReturn(subscription);
        return subscription;
    }

}
