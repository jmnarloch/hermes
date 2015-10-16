package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.awaitility.Duration;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.ONE_SECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class BalancedWorkloadSupervisorControllersIntegrationTest extends ZookeeperBaseTest {

    private static ConsumersSupervisor supervisor = mock(ConsumersSupervisor.class);
    private static SubscriptionsCache subscriptionsCache = mock(SubscriptionsCache.class);
    private static SubscriptionRepository subscriptionsRepository = mock(SubscriptionRepository.class);

    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static ConsumersRegistry consumersRegistry = new ConsumersRegistry(zookeeperClient, "/registry", "id");

    @Test
    public void shouldRegisterConsumerOnStartup() throws Exception {
        // given
        String id = "supervisor1";

        // when
        getConsumerSupervisor(id).start();

        // then
        waitForRegistration(id);
    }

    @Test
    public void shouldElectOnlyOneLeaderFromRegisteredConsumers() {
        // given
        List<BalancedWorkloadSupervisorController> supervisors = ImmutableList.of(
                getConsumerSupervisor("1"), getConsumerSupervisor("2"), getConsumerSupervisor("3"));

        // when
        supervisors.forEach(this::startConsumer);

        // then
        assertThat(supervisors.stream().filter(BalancedWorkloadSupervisorController::isLeader).count()).isEqualTo(1);
    }

    @Test
    public void shouldElectNewLeaderAfterShutdown() throws InterruptedException {
        // given
        Map<String, CuratorFramework> curators = ImmutableMap.of("A", otherClient(), "B", otherClient());
        List<BalancedWorkloadSupervisorController> supervisors = curators.entrySet().stream()
                .map(entry -> getConsumerSupervisor(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        supervisors.forEach(this::startConsumer);
        BalancedWorkloadSupervisorController leader = findLeader(supervisors);

        // when
        curators.get(leader.getId()).close();
        await().atMost(ONE_SECOND).until(() -> !consumersRegistry.isRegistered(leader.getId()));

        // then
        await().atMost(Duration.TEN_SECONDS).until(() -> supervisors.stream()
                .filter(BalancedWorkloadSupervisorController::isLeader)
                .filter(c -> !c.equals(leader))
                .findAny().isPresent());

        await().atMost(Duration.ONE_SECOND).until(() -> !leader.isLeader());
    }

    @Test
    public void shouldAssignConsumerToSubscription() {
        // given
        BalancedWorkloadSupervisorController controller = getConsumerSupervisor("c1");
        startConsumer(controller);
        Subscription subscription = Subscription.fromSubscriptionName(SubscriptionName.fromString("com.example.topic$test"));

        // when
        controller.onSubscriptionCreated(subscription);

        // then
        await().atMost(Duration.TEN_SECONDS).until(() -> zookeeperClient.checkExists().forPath("/runtime/example$test/c1") != null);
    }

    @Test
    public void shouldAssignSubscriptionToMultipleConsumers() {
        List<BalancedWorkloadSupervisorController> supervisors = ImmutableList.of(
                getConsumerSupervisor("d1"), getConsumerSupervisor("d2"));
        supervisors.forEach(this::startConsumer);
        Subscription subscription = Subscription.fromSubscriptionName(SubscriptionName.fromString("com.example.topic$test"));

        // when
        supervisors.forEach(c -> c.onSubscriptionCreated(subscription));

        // then
        await().atMost(Duration.TEN_SECONDS).until(() ->
                           zookeeperClient.checkExists().forPath("/runtime/example$test/d1") != null
                        && zookeeperClient.checkExists().forPath("/runtime/example$test/d2") != null);
    }

    @Test
    public void shouldNotExceedConsumerLimitWhenAssigningSubscriptions() {
        // given
        BalancedWorkloadSupervisorController controller = getConsumerSupervisor("c1");
        startConsumer(controller);
        Subscription subscription1 = Subscription.fromSubscriptionName(SubscriptionName.fromString("com.example.topic$test1"));
        Subscription subscription2 = Subscription.fromSubscriptionName(SubscriptionName.fromString("com.example.topic$test2"));
        controller.onSubscriptionCreated(subscription1);
        await().atMost(Duration.ONE_SECOND).until(() -> zookeeperClient.checkExists().forPath("/runtime/example$test1/c1") != null);

        // when
        controller.onSubscriptionCreated(subscription2);

        // then
        await().atMost(Duration.ONE_SECOND).until(() -> zookeeperClient.checkExists().forPath("/runtime/example$test2/c1") != null);
    }

    private BalancedWorkloadSupervisorController findLeader(List<BalancedWorkloadSupervisorController> supervisors) {
        return supervisors.stream()
                .filter(BalancedWorkloadSupervisorController::isLeader).findAny().get();
    }

    private void startConsumer(BalancedWorkloadSupervisorController supervisorController) {
        try {
            supervisorController.start();
            waitForRegistration(supervisorController.getId());
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }

    private void waitForRegistration(String id) {
        await().atMost(ONE_SECOND).until(() -> consumersRegistry.isRegistered(id));
    }

    private static BalancedWorkloadSupervisorController getConsumerSupervisor(String id) {
        return getConsumerSupervisor(id, otherClient());
    }

    private static BalancedWorkloadSupervisorController getConsumerSupervisor(String id, CuratorFramework curator) {
        WorkTracker workTracker = new WorkTracker(curator, new ObjectMapper(), "/runtime", id, executorService, subscriptionsRepository);
        return new BalancedWorkloadSupervisorController(supervisor, subscriptionsCache, workTracker, new ConsumersRegistry(curator, "/registry", id), new WorkBalancer(), id);
    }
}
