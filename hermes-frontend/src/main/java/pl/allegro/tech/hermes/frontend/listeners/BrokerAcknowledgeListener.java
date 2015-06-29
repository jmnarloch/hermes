package pl.allegro.tech.hermes.frontend.listeners;

import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.frontend.publishing.Message;

public interface BrokerAcknowledgeListener {

    void onAcknowledge(Message message, Topic topic);
}
