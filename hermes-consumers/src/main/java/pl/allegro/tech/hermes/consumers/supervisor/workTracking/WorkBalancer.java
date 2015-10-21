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

        workload.addNewSubscriptions(subscriptions);
        workload.addNewSupervisors(supervisors);

        workload.assignSupervisors();

//        workload.rebalance all the things ?

        return workload.asSubscriptionAssignmentView();
    }
}
