package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import org.apache.curator.framework.CuratorFramework;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;

public class ConsumersRegistry {

    private final CuratorFramework curatorClient;
    private final String prefix;

    public ConsumersRegistry(CuratorFramework curatorClient, String prefix) {
        this.curatorClient = curatorClient;
        this.prefix = prefix;
    }

    public void register(String supervisorId) {
        try {
            curatorClient.create().creatingParentsIfNeeded()
                    .withMode(EPHEMERAL).forPath(getPath(supervisorId));
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }

    public boolean isRegistered(String supervisorId) {
        try {
            return curatorClient.checkExists().forPath(getPath(supervisorId)) != null;
        } catch (Exception e) {
            throw new InternalProcessingException(e);
        }
    }

    private String getPath(String supervisorId) {
        return prefix + "/" + supervisorId;
    }
}
