package com.aw.micro.recommendation_service.configuration;

import com.aw.micro.api.core.recommendation.Recommendation;
import com.aw.micro.api.core.recommendation.RecommendationService;
import com.aw.micro.api.event.Event;
import com.aw.micro.api.exceptions.EventProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class MessageProcessorConfig {

    private static final Logger LOG= LoggerFactory.getLogger(MessageProcessorConfig.class);

    private final RecommendationService recommendationService;

    @Autowired
    public MessageProcessorConfig(RecommendationService recommendationService){
        this.recommendationService=recommendationService;
    }

    @Bean
    public Consumer<Event<Integer, Recommendation>> messageProcessor(){
        return event->{


            LOG.info("Process message crated at{}...",event.getEventCreatedAt());

            switch (event.getEventType()){
                case CREATE :
                    Recommendation recommendation = event.getData();
                    LOG.info("Create recommendation with ID: {}/{}", recommendation.getProductId(), recommendation.getRecommendationId());
                    recommendationService.createRecommendation(recommendation).block();//Remove block!!
                    break;

                case DELETE:
                    int productId = event.getKey();
                    LOG.info("Delete recommendations with ProductID: {}", productId);
                    recommendationService.deleteRecommendation(productId).block();
                    break;

                default:
                    String errorMessage = "Incorrect event type: " + event.getEventType() + ", expected a CREATE or DELETE event";
                    LOG.warn(errorMessage);
                    throw new EventProcessingException(errorMessage);
            }
            LOG.info("Message processing done!");


        };
    }
}
