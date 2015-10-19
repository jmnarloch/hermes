package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionAssignmentViewTest {

    @Test
    public void shouldNotDeleteFromEmptyView() {
        // given
        SubscriptionName s1 = anySubscriptionName();
        SubscriptionAssignmentView current = subscriptionAssignmentView(s1, Collections.emptySet());
        SubscriptionAssignmentView target = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));

        // when
        SubscriptionAssignmentView deletions = current.deletions(target);

        // then
        assertThat(deletions.getSubscriptionSet()).isEmpty();
    }

    @Test
    public void shouldAddToEmptyView() {
        // given
        SubscriptionName s1 = anySubscriptionName();
        SubscriptionAssignmentView current = subscriptionAssignmentView(s1, Collections.emptySet());
        SubscriptionAssignmentView target = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));

        // when
        SubscriptionAssignmentView additions = current.additions(target);

        // then
        assertThat(additions).isEqualTo(target);
    }

    @Test
    public void shouldNotAddToUnchangedView() {
        // given
        SubscriptionName s1 = anySubscriptionName();
        SubscriptionAssignmentView current = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));
        SubscriptionAssignmentView target = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));

        // when
        SubscriptionAssignmentView additions = current.additions(target);

        // then
        assertThat(additions.getSubscriptionSet()).isEmpty();
    }

    @Test
    public void shouldNotDeleteFromUnchangedView() {
        // given
        SubscriptionName s1 = anySubscriptionName();
        SubscriptionAssignmentView current = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));
        SubscriptionAssignmentView target = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));

        // when
        SubscriptionAssignmentView deletions = current.deletions(target);

        // then
        assertThat(deletions.getSubscriptionSet()).isEmpty();
    }

    @Test
    public void shouldAddAssignmentToExistingSubscription() {
        // given
        SubscriptionName s1 = anySubscriptionName();
        SubscriptionAssignmentView current = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));
        SubscriptionAssignmentView target = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1"), assignment(s1, "c2")));

        // when
        SubscriptionAssignmentView additions = current.additions(target);

        // then
        assertThat(additions.getAssignments(s1)).containsOnly(assignment(s1, "c2"));
    }

    @Test
    public void shouldAddNewSubscription() {
        // given
        SubscriptionName s1 = anySubscriptionName();
        SubscriptionName s2 = anySubscriptionName();
        SubscriptionAssignmentView current = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));
        SubscriptionAssignmentView target = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")), s2, ImmutableSet.of(assignment(s2, "c1")));

        // when
        SubscriptionAssignmentView additions = current.additions(target);

        // then
        assertThat(additions.getSubscriptionSet()).containsOnly(s2);
        assertThat(additions.getAssignments(s2)).containsOnly(assignment(s2, "c1"));
    }

    @Test
    public void shouldDeleteOldSubscription() {
        // given
        SubscriptionName s1 = anySubscriptionName();
        SubscriptionAssignmentView current = subscriptionAssignmentView(s1, ImmutableSet.of(assignment(s1, "c1")));
        SubscriptionAssignmentView target = subscriptionAssignmentView(s1, Collections.emptySet());

        // when
        SubscriptionAssignmentView deletions = current.deletions(target);

        // then
        assertThat(deletions.getSubscriptionSet()).isEqualTo(current.getSubscriptionSet());
        assertThat(deletions.getAssignments(s1)).isEqualTo(current.getAssignments(s1));
    }

    private SubscriptionAssignmentView subscriptionAssignmentView(SubscriptionName s1, Set<SubscriptionAssignment> assignments) {
        return new SubscriptionAssignmentView(ImmutableMap.of(s1, assignments));
    }

    private SubscriptionAssignmentView subscriptionAssignmentView(SubscriptionName s1, Set<SubscriptionAssignment> assignments1, SubscriptionName s2, Set<SubscriptionAssignment> assignments2) {
        return new SubscriptionAssignmentView(ImmutableMap.of(s1, assignments1, s2, assignments2));
    }

    private SubscriptionAssignment assignment(SubscriptionName s1, String supervisorId) {
        return new SubscriptionAssignment(supervisorId, s1);
    }

    private SubscriptionName anySubscriptionName() {
        return SubscriptionName.fromString("com.example.topic$" + Math.abs(UUID.randomUUID().getMostSignificantBits()));
    }
}