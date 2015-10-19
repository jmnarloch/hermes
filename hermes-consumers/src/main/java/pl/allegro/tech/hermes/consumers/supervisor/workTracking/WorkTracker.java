package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.common.cache.zookeeper.NodeCache;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;
import pl.allegro.tech.hermes.domain.subscription.SubscriptionRepository;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;

public class WorkTracker extends NodeCache<SubscriptionAssignmentAware, SubscriptionAssignmentRegistry> {
    private final SubscriptionRepository subscriptionRepository;
    private final String supervisorId;
    private final SubscriptionAssignmentPathSerializer pathSerializer;

public WorkTracker(CuratorFramework curatorClient,
                       ObjectMapper objectMapper,
                       String path,
                       String supervisorId,
                       ExecutorService executorService,
                       SubscriptionRepository subscriptionRepository) {
        super(curatorClient, objectMapper, path, executorService);
        this.subscriptionRepository = subscriptionRepository;
        this.supervisorId = supervisorId;
        this.pathSerializer = new SubscriptionAssignmentPathSerializer(path, supervisorId);
    }

    public void forceAssignment(Subscription subscription) {
        askCuratorPolitely(() ->
                curatorClient.create().creatingParentsIfNeeded().withMode(EPHEMERAL).forPath(pathSerializer.serialize(subscription)));
    }

    public void dropAssignment(Subscription subscription) {
        askCuratorPolitely(() ->
                curatorClient.delete().guaranteed().forPath(pathSerializer.serialize(subscription)));
    }

    private void askCuratorPolitely(CuratorTask task) {
        try {
            task.run();
        } catch (KeeperException.NodeExistsException | KeeperException.NoNodeException ex) {
            // ignore
        } catch (Exception ex) {
            throw new InternalProcessingException(ex);
        }
    }

    public void apply(SubscriptionAssignmentView work) {

    }

    public Set<SubscriptionAssignment> getAssignments(Subscription subscription) {
        return getAssignments(subscription.toSubscriptionName().toString());
    }

    private Set<SubscriptionAssignment> getAssignments(String subscriptionName) {
        return getEntry(subscriptionName).getCurrentData().stream()
                .map(child -> pathSerializer.deserialize(child.getPath())).collect(Collectors.toSet());
    }

    public SubscriptionAssignmentView getAssignments() {
        return new SubscriptionAssignmentView(getSubcacheKeySet().stream().collect(toMap(SubscriptionName::fromString, this::getAssignments)));
    }

    interface CuratorTask {
        void run() throws Exception;
    }

    @Override
    protected SubscriptionAssignmentRegistry createSubcache(String path) {
        return new SubscriptionAssignmentRegistry(
                curatorClient,
                path,
                executorService,
                subscriptionRepository,
                supervisorId,
                pathSerializer);
    }
}
