package pl.allegro.tech.hermes.consumers.supervisor.workload;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.allegro.tech.hermes.api.Group;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.subscription.cache.zookeeper.ZookeeperSubscriptionsCacheFactory;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;
import pl.allegro.tech.hermes.consumers.supervisor.workload.selective.ConsumerNodesRegistry;
import pl.allegro.tech.hermes.consumers.supervisor.workload.selective.SelectiveSupervisorController;
import pl.allegro.tech.hermes.domain.group.GroupRepository;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.domain.topic.TopicRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperGroupRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperSubscriptionRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperTopicRepository;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.ONE_SECOND;
import static com.jayway.awaitility.Duration.TEN_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static pl.allegro.tech.hermes.api.Topic.Builder.topic;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_REBALANCE_INTERVAL;
import static pl.allegro.tech.hermes.test.helper.endpoint.TimeoutAdjuster.adjust;

public class SelectiveSupervisorControllersIntegrationTest extends ZookeeperBaseTest {

    private ZookeeperPaths paths;
    private GroupRepository groupRepository;
    private TopicRepository topicRepository;
    private SubscriptionRepository subscriptionRepository;

    private ConsumersSupervisor supervisor = mock(ConsumersSupervisor.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    private ConfigFactory configFactory;
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ConsumerNodesRegistry consumersRegistry;
    private HermesMetrics metrics;


    @Before
    public void setup() throws Exception {
        configFactory = new MutableConfigFactory().overrideProperty(CONSUMER_WORKLOAD_REBALANCE_INTERVAL, 1);
        zookeeperClient.create().creatingParentsIfNeeded().forPath("/hermes/groups");
        paths = new ZookeeperPaths("/hermes");

        consumersRegistry = new ConsumerNodesRegistry(zookeeperClient, executorService, paths.consumersRegistryPath(), "id");
        consumersRegistry.start();

        groupRepository = new ZookeeperGroupRepository(zookeeperClient, objectMapper, paths);
        topicRepository = new ZookeeperTopicRepository(zookeeperClient, objectMapper, paths, groupRepository);
        subscriptionRepository = new ZookeeperSubscriptionRepository(zookeeperClient, objectMapper, paths, topicRepository);
        metrics = mock(HermesMetrics.class);
        when(metrics.timer(anyString())).thenReturn(new Timer());
    }

    @After
    public void cleanup() throws Exception {
        deleteAllNodes();
    }

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
        List<SelectiveSupervisorController> supervisors = ImmutableList.of(
                getConsumerSupervisor("1"), getConsumerSupervisor("2"), getConsumerSupervisor("3"));

        // when
        supervisors.forEach(this::startConsumer);

        // then
        assertThat(supervisors.stream().filter(SelectiveSupervisorController::isLeader).count()).isEqualTo(1);
    }

    @Test
    public void shouldElectNewLeaderAfterShutdown() throws InterruptedException {
        // given
        Map<String, CuratorFramework> curators = ImmutableMap.of("A", otherClient(), "B", otherClient());
        List<SelectiveSupervisorController> supervisors = curators.entrySet().stream()
                .map(entry -> getConsumerSupervisor(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        supervisors.forEach(this::startConsumer);
        SelectiveSupervisorController leader = findLeader(supervisors);

        // when
        curators.get(leader.getId()).close();
        await().atMost(adjust(ONE_SECOND)).until(() -> !consumersRegistry.isRegistered(leader.getId()));

        // then
        await().atMost(adjust(TEN_SECONDS)).until(() -> supervisors.stream()
                .filter(SelectiveSupervisorController::isLeader)
                .filter(c -> !c.equals(leader))
                .findAny().isPresent());

        await().atMost(adjust(ONE_SECOND)).until(() -> !leader.isLeader());
    }

    @Test
    public void shouldAssignConsumerToSubscription() {
        // given
        SelectiveSupervisorController controller = getConsumerSupervisor("c1");
        startConsumer(controller);

        // when
        createSubscription("com.example.topic$test");

        // then
        awaitUntilAssignmentExists("com.example.topic$test", "c1");
    }

    @Test
    public void shouldAssignSubscriptionToMultipleConsumers() {
        // given
        ImmutableList.of(getConsumerSupervisor("c1"), getConsumerSupervisor("c2")).forEach(this::startConsumer);

        // when
        createSubscription("com.example.topic$test");

        // then
        awaitUntilAssignmentExists("com.example.topic$test", "c1");
        awaitUntilAssignmentExists("com.example.topic$test", "c1");
    }

    @Test
    public void shouldAssignConsumerToMultipleSubscriptions() {
        // given
        startConsumer(getConsumerSupervisor("c1"));

        // when
        createSubscription("com.example.topic$test1");
        createSubscription("com.example.topic$test2");

        // then
        awaitUntilAssignmentExists("com.example.topic$test1", "c1");
        awaitUntilAssignmentExists("com.example.topic$test2", "c1");
    }

    private void awaitUntilAssignmentExists(String subscription, String supervisorId) {
        await().atMost(adjust(ONE_SECOND)).until(() -> zookeeperClient.checkExists().forPath(assignmentPath(subscription, supervisorId)) != null);
    }

    private String assignmentPath(String subscription, String supervisorId) {
        return paths.consumersRuntimePath() + "/" + subscription + "/" + supervisorId;
    }

    private SelectiveSupervisorController getConsumerSupervisor(String id) {
        return getConsumerSupervisor(id, otherClient());
    }

    private SelectiveSupervisorController getConsumerSupervisor(String id, CuratorFramework curator) {
        WorkTracker workTracker = new WorkTracker(curator, objectMapper, paths.consumersRuntimePath(), id, executorService, subscriptionRepository);
        ConsumerNodesRegistry registry = new ConsumerNodesRegistry(curator, executorService, paths.consumersRegistryPath(), id);
        SubscriptionsCache subscriptionsCache = new ZookeeperSubscriptionsCacheFactory(curator, configFactory, objectMapper).provide();
        return new SelectiveSupervisorController(supervisor, subscriptionsCache, workTracker, registry, configFactory, metrics);
    }

    private void startConsumer(SelectiveSupervisorController supervisorController) {
        try {
            supervisorController.start();
            waitForRegistration(supervisorController.getId());
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }

    private Subscription createSubscription(String subscriptionName) {
        Subscription subscription = Subscription.fromSubscriptionName(SubscriptionName.fromString(subscriptionName));
        Group group = Group.from(subscription.getTopicName().getGroupName());
        if (!groupRepository.groupExists(group.getGroupName())) {
            groupRepository.createGroup(group);
        }
        if (!topicRepository.topicExists(subscription.getTopicName())) {
            topicRepository.createTopic(topic().applyDefaults().withName(subscription.getTopicName()).build());
        }
        subscriptionRepository.createSubscription(subscription);
        await().atMost(adjust(ONE_SECOND)).until(() -> subscriptionRepository.subscriptionExists(subscription.getTopicName(), subscription.getName()));
        return subscription;
    }

    private SelectiveSupervisorController findLeader(List<SelectiveSupervisorController> supervisors) {
        return supervisors.stream()
                .filter(SelectiveSupervisorController::isLeader).findAny().get();
    }

    private void waitForRegistration(String id) {
        await().atMost(adjust(ONE_SECOND)).until(() -> consumersRegistry.isRegistered(id));
    }
}
