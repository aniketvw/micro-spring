package com.aw.micro.review_service.services;

import com.aw.micro.api.core.review.Review;
import com.aw.micro.api.core.review.ReviewService;
import com.aw.micro.api.exceptions.InvalidInputException;
import com.aw.micro.review_service.persistence.ReviewEntity;
import com.aw.micro.review_service.persistence.ReviewRepository;
import com.aw.micro.util.http.ServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;


import java.util.List;
import java.util.logging.Level;

@RestController
public class ReviewServiceImpl implements ReviewService {

    private static final Logger LOG = LoggerFactory.getLogger(ReviewServiceImpl.class);

    private final ServiceUtil serviceUtil;
    private final ReviewMapper mapper;
    private final ReviewRepository repository;
    private final Scheduler jdbcScheduler;


    @Autowired
    public ReviewServiceImpl(@Qualifier("jdbcSchedular") Scheduler jdbcScheduler, ServiceUtil serviceUtil, ReviewMapper mapper, ReviewRepository repository) {
        this.jdbcScheduler = jdbcScheduler;
        this.serviceUtil = serviceUtil;
        this.mapper = mapper;
        this.repository = repository;
    }

    @Override
    public Flux<Review> getReviews(int productId) {

        if (productId < 1) {
            throw new InvalidInputException("Invalid productId: " + productId);
        }

        LOG.info("Will get reviews for product with id={}", productId);

        return Mono.fromCallable(() -> internalGetReviews(productId))
                .flatMapMany(Flux::fromIterable)
                .log(LOG.getName(), Level.FINE)
                .subscribeOn(jdbcScheduler);
    }

    private List<Review> internalGetReviews(int productId) {
        List<ReviewEntity> entity = repository.findByProductId(productId);
        List<Review> reviewList = mapper.entityListToApiList(entity);
        reviewList.forEach(e -> e.setServiceAddress(serviceUtil.getServiceAddress()));
        LOG.debug("Response size: {}", reviewList.size());

        return reviewList;
    }

    @Override
    public Mono<Review> createReview(Review body) {

        return Mono.fromCallable(()->internalCreateReview(body))
                .subscribeOn(jdbcScheduler);

    }

    private Review internalCreateReview(Review body) {

        try {
            ReviewEntity reviewEntity = mapper.apiToEntity(body);
            ReviewEntity newEntity = repository.save(reviewEntity);

            LOG.debug("createReview: created a review entity: {}/{}", body.getProductId(), body.getReviewId());
            return mapper.entityToApi(newEntity);

        } catch (DataIntegrityViolationException dive) {
            throw new InvalidInputException("Duplicate key, Product Id: " + body.getProductId() + ", Review Id:" + body.getReviewId());
        }

    }


    @Override
    public Mono<Void> deleteReviews(int productId) {
        if (productId < 1) {
            throw new InvalidInputException("Invalid productId: " + productId);
        }
        return Mono.fromRunnable(()->internalDeleteReviews(productId))
                .subscribeOn(jdbcScheduler)
                .then();
    }

    private void internalDeleteReviews(int productId){
        LOG.debug("deleteReviews: tries to delete reviews for the product with productId: {}", productId);

        repository.deleteAll(repository.findByProductId(productId));

    }
}
