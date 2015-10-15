package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class BalancedWorkloadSupervisorControllersIntegrationTest extends ZookeeperBaseTest {

    private static ConsumersSupervisor supervisor = mock(ConsumersSupervisor.class);
    private static SubscriptionsCache subscriptionsCache = mock(SubscriptionsCache.class);
    private static SubscriptionRepository subscriptionsRepository = mock(SubscriptionRepository.class);

    private static List<BalancedWorkloadSupervisorController> controllers = new ArrayList<>();

    static ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static ConsumersRegistry consumersRegistry = new ConsumersRegistry();

    @BeforeClass
    public static void setup() throws Exception {
        int nConsumers = 3;
        for (int i = 0; i < nConsumers; i++) {
            BalancedWorkloadSupervisorController controller = getBalancedWorkloadSupervisorController("c" + i, i == 0 ? zookeeperClient : otherClient());
            controllers.add(controller);
            controller.start();
        }
    }

    @Test
    public void shouldRegisterConsumerOnStartup() throws Exception {
        // given
        String id = "supervisor1";

        // when
        getBalancedWorkloadSupervisorController(id, otherClient()).start();

        // then
        assertThat(consumersRegistry.isRegistered(id)).isTrue();
    }

    private static BalancedWorkloadSupervisorController getBalancedWorkloadSupervisorController(String id, CuratorFramework curator) {
        WorkTracker workTracker = new WorkTracker(curator, new ObjectMapper(), "/runtime", id, executorService, subscriptionsRepository);
        return new BalancedWorkloadSupervisorController(supervisor, subscriptionsCache, workTracker, consumersRegistry);
    }
}
