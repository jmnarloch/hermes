package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.ONE_SECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class BalancedWorkloadSupervisorControllersIntegrationTest extends ZookeeperBaseTest {

    private static ConsumersSupervisor supervisor = mock(ConsumersSupervisor.class);
    private static SubscriptionsCache subscriptionsCache = mock(SubscriptionsCache.class);
    private static SubscriptionRepository subscriptionsRepository = mock(SubscriptionRepository.class);

//    private static List<BalancedWorkloadSupervisorController> controllers = new ArrayList<>();

    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static ConsumersRegistry consumersRegistry = new ConsumersRegistry(zookeeperClient, "/registry", "id");

//    @BeforeClass
//    public static void setup() throws Exception {
//        int nConsumers = 3;
//        for (int i = 0; i < nConsumers; i++) {
//            BalancedWorkloadSupervisorController controller = runWithSupervisor("c" + i, i == 0 ? zookeeperClient : otherClient());
//            controllers.add(controller);
//            controller.start();
//        }
//    }

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
        CuratorFramework curator = otherClient();
        WorkTracker workTracker = new WorkTracker(curator, new ObjectMapper(), "/runtime", id, executorService, subscriptionsRepository);
        return new BalancedWorkloadSupervisorController(supervisor, subscriptionsCache, workTracker, new ConsumersRegistry(curator, "/registry", id), id);
    }
}
