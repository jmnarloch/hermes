package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;

import java.io.IOException;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;

public class ConsumersRegistry {

    private final CuratorFramework curatorClient;
    private String supervisorId;
    private final String prefix;
    private final LeaderLatch leaderLatch;

    public ConsumersRegistry(CuratorFramework curatorClient, String prefix, String supervisorId) {
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
        return prefix + "/nodes/" + supervisorId;
    }

    private String getLeaderPath() {
        return prefix + "/leader";
    }

    public boolean isLeader() {
        return curatorClient.getState() == CuratorFrameworkState.STARTED && leaderLatch.hasLeadership();
    }
}
