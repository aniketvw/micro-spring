package com.aw.micro.product_composite_service.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Configuration
public class ThreadConfig {

    private static final Logger LOG= LoggerFactory.getLogger(ThreadConfig.class);

    private final Integer threadPoolSize;
    private final Integer taskQueueSize;

    @Autowired
    public ThreadConfig(@Value("${app.threadPoolSize:10}") Integer threadPoolSize,
                        @Value("${app.taskQueueSize:100}") Integer taskQueueSize
    ) {
        this.taskQueueSize=taskQueueSize;
        this.threadPoolSize=threadPoolSize;
    }

    @Bean
    public Scheduler publishEventScheduler(){
        LOG.info("Created a messagingScheduler with connectionPoolSize = {}", threadPoolSize);
        return Schedulers.newBoundedElastic(threadPoolSize,taskQueueSize,"publish-pool");
    }

}
