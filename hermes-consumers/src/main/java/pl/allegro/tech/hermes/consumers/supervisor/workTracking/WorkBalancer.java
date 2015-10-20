package pl.allegro.tech.hermes.consumers.supervisor.workTracking;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.List;

public class WorkBalancer {

    public SubscriptionAssignmentView balance(List<SubscriptionName> subscriptions,
                                              List<String> supervisors,
                                              SubscriptionAssignmentView currentState) {

        MutableWorkloadView workload = new MutableWorkloadView(currentState);
        workload.removeInvalidSubscriptions(subscriptions);
        workload.removeInvalidSupervisors(supervisors);

        return workload.asSubscriptionAssignmentView();
    }
}
