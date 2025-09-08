package com.aw.micro.review_service.configuration;

import com.aw.micro.api.core.review.Review;
import com.aw.micro.api.core.review.ReviewService;
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

    private final ReviewService reviewService;

    @Autowired
    public MessageProcessorConfig(ReviewService reviewService){
        this.reviewService=reviewService;
    }

    @Bean
    public Consumer<Event<Integer, Review>> messageProcessor(){


        return event->{
            LOG.info("Process message created at {}...", event.getEventCreatedAt());

            switch (event.getEventType()) {

                case CREATE:
                    Review review = event.getData();
                    LOG.info("Create review with ID: {}/{}", review.getProductId(), review.getReviewId());
                    reviewService.createReview(review).block();//Remove block!!! page 213
                    break;

                case DELETE:
                    int productId = event.getKey();
                    LOG.info("Delete reviews with ProductID: {}", productId);
                    reviewService.deleteReviews(productId).block();//Remove block!!!
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
