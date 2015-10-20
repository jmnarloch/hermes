package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import pl.allegro.tech.hermes.common.cache.zookeeper.StartableCache;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.StringUtils.substringAfterLast;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;

public class ConsumersRegistry extends StartableCache {

    private final CuratorFramework curatorClient;
    private String supervisorId;
    private final String prefix;
    private final LeaderLatch leaderLatch;

    public ConsumersRegistry(CuratorFramework curatorClient, ExecutorService executorService, String prefix, String supervisorId) {
        super(curatorClient, getNodesPath(prefix), executorService);
        this.curatorClient = curatorClient;
        this.supervisorId = supervisorId;
        this.prefix = prefix;
        this.leaderLatch = new LeaderLatch(curatorClient, getLeaderPath());
    }

    public void register() {
        try {
            curatorClient.create().creatingParentsIfNeeded()
                    .withMode(EPHEMERAL).forPath(getNodePath(supervisorId));
            leaderLatch.start();
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }

    public boolean isRegistered(String supervisorId) {
        try {
            return curatorClient.checkExists().forPath(getNodePath(supervisorId)) != null;
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }

    private String getNodePath(String supervisorId) {
        return getNodesPath(prefix) + "/" + supervisorId;
    }

    private static String getNodesPath(String prefix) {
        return prefix + "/nodes";
    }

    private String getLeaderPath() {
        return prefix + "/leader";
    }

    public boolean isLeader() {
        return curatorClient.getZookeeperClient().isConnected() && leaderLatch.hasLeadership();
    }

    public List<String> list() {
        return getCurrentData().stream().map(data -> substringAfterLast(data.getPath(), "/")).collect(toList());
    }
}
