package pl.allegro.tech.hermes.management.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pl.allegro.tech.hermes.management.domain.subscription.SubscriptionLagSource;
import pl.allegro.tech.hermes.management.infrastructure.graphite.GraphiteClient;
import pl.allegro.tech.hermes.management.infrastructure.metrics.NoOpSubscriptionLagSource;
import pl.allegro.tech.hermes.management.stub.MetricsPaths;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.time.Clock;

@Configuration
@EnableConfigurationProperties({TopicProperties.class, MetricsProperties.class, HttpClientProperties.class})
public class ManagementConfiguration {

    @Autowired
    MetricsProperties metricsProperties;

    @Autowired
    HttpClientProperties httpClientProperties;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.WRITE_NULL_MAP_VALUES);
        return mapper;
    }

    @Bean
    public Client jerseyClient() {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, httpClientProperties.getConnectTimeout());
        config.property(ClientProperties.READ_TIMEOUT, httpClientProperties.getReadTimeout());

        return ClientBuilder.newClient(config);
    }

    @Bean
    public MetricsPaths metricsPaths() {
        return new MetricsPaths(metricsProperties.getPrefix());
    }

    @Bean
    public GraphiteClient graphiteClient() {
        return new GraphiteClient(jerseyClient().target(metricsProperties.getGraphiteHttpUri()));
    }

    @Bean
    @ConditionalOnMissingBean
    public SubscriptionLagSource consumerLagSource() {
        return new NoOpSubscriptionLagSource();
    }

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}
