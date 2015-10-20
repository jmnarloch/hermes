package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableList;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import pl.allegro.tech.hermes.api.SubscriptionName;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkBalancerTest {
    private WorkBalancer workBalancer = new WorkBalancer();


    @Test
    public void shouldBalanceWorkForSingleSubscription() {
        // given
        SubscriptionName s = SubscriptionName.fromString("tech.topic$s1");
        String supervisor = "c1";

        // when
        SubscriptionAssignmentView view = workBalancer.balance(ImmutableList.of(s),
                ImmutableList.of(supervisor), new SubscriptionAssignmentView(ImmutableMap.of()));

        // then
        assertThat(view.getAssignments(s)).extracting(SubscriptionAssignment::getSupervisorId).isEqualTo(supervisor);
    }
}
