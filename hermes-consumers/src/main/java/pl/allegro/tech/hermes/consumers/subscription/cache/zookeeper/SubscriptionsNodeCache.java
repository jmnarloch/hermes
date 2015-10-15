package pl.allegro.tech.hermes.consumers.subscription.cache.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.cache.zookeeper.StartableCache;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionCallback;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.substringAfterLast;

class SubscriptionsNodeCache extends StartableCache<SubscriptionCallback> implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionsNodeCache.class);

    private final ObjectMapper objectMapper;

    public SubscriptionsNodeCache(CuratorFramework client, ObjectMapper objectMapper,
                                  String path, ExecutorService executorService) {

        super(client, path, executorService);
        this.objectMapper = objectMapper;
        getListenable().addListener(this);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (event.getData() == null || event.getData().getData() == null) {
            LOGGER.warn("Unrecognized event {}", event);
            return;
        }
        String path = event.getData().getPath();
        Subscription subscription = readSubscription(event);
        LOGGER.info("Got subscription change event for path {} type {}", path, event.getType());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Event data {}", new String(event.getData().getData(), Charsets.UTF_8));
        }
        switch (event.getType()) {
            case CHILD_ADDED:
                for (SubscriptionCallback callback : callbacks) {
                    callback.onSubscriptionCreated(subscription);
                }
                break;
            case CHILD_REMOVED:
                for (SubscriptionCallback callback : callbacks) {
                    callback.onSubscriptionRemoved(subscription);
                }
                break;
            case CHILD_UPDATED:
                for (SubscriptionCallback callback : callbacks) {
                    callback.onSubscriptionChanged(subscription);
                }
                break;
            default:
                break;
        }
    }

    private Subscription readSubscription(PathChildrenCacheEvent event) throws IOException {
        return objectMapper.readValue(event.getData().getData(), Subscription.class);
    }

    public List<SubscriptionName> listSubscriptionNames(String group, String topic) {
        return getCurrentData().stream()
                .map(data -> new SubscriptionName(substringAfterLast(data.getPath(), "/"), new TopicName(group, topic)))
                .collect(Collectors.toList());
    }
}
