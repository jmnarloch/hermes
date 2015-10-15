package pl.allegro.tech.hermes.consumers.supervisor.workload;

import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.glassfish.hk2.api.Factory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.di.CuratorType;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.consumers.supervisor.ConsumersSupervisor;
import pl.allegro.tech.hermes.consumers.supervisor.workload.mirror.LegacyMirroringSupervisorController;
import pl.allegro.tech.hermes.consumers.supervisor.workload.mirror.MirroringSupervisorController;
import pl.allegro.tech.hermes.consumers.supervisor.workload.selective.ConsumerNodesRegistry;
import pl.allegro.tech.hermes.consumers.supervisor.workload.selective.SelectiveSupervisorController;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.Map;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_ALGORITHM;

public class SupervisorControllerFactory implements Factory<SupervisorController> {
    private final ConfigFactory configs;
    private final Map<String, Provider<SupervisorController>> availableImplementations;

    @Inject
    public SupervisorControllerFactory(@Named(CuratorType.HERMES) CuratorFramework curator,
                                       SubscriptionsCache subscriptionsCache,
                                       WorkTracker workTracker,
                                       ConsumersSupervisor supervisor,
                                       HermesMetrics metrics,
                                       ConfigFactory configs) {
        this.configs = configs;
        this.availableImplementations = ImmutableMap.of(
                "legacy.mirror", () -> new LegacyMirroringSupervisorController(supervisor, subscriptionsCache, configs),
                "mirror", () -> new MirroringSupervisorController(supervisor, subscriptionsCache, workTracker, configs),
                "selective", () -> new SelectiveSupervisorController(supervisor, subscriptionsCache, workTracker,
                                                                  createConsumersRegistry(configs, curator), configs, metrics));
    }

    private static ConsumerNodesRegistry createConsumersRegistry(ConfigFactory configs, CuratorFramework curator) {
        return new ConsumerNodesRegistry(curator,
                newSingleThreadExecutor(),
                new ZookeeperPaths(configs.getStringProperty(Configs.ZOOKEEPER_ROOT)).consumersRegistryPath(),
                configs.getStringProperty(Configs.CONSUMER_WORKLOAD_NODE_ID));
    }

    @Override
    public SupervisorController provide() {
        return availableImplementations.get(configs.getStringProperty(CONSUMER_WORKLOAD_ALGORITHM)).get();
    }

    @Override
    public void dispose(SupervisorController instance) {

    }
}
