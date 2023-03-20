package org.databiosphere.workspacedataservice;

import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;


@Component
@Profile("!local, !mock-sam")
public class InstanceInitializer implements
        ApplicationListener<ContextRefreshedEvent> {


    public InstanceInitializer(){

    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
    }

}
