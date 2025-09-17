package com.aw.micro.recommendation_service.service;

import com.aw.micro.api.core.recommendation.Recommendation;
import com.aw.micro.api.core.recommendation.RecommendationService;
import com.aw.micro.api.exceptions.InvalidInputException;
import com.aw.micro.recommendation_service.persistence.RecommendationEntity;
import com.aw.micro.recommendation_service.persistence.RecommendationRepository;
import com.aw.micro.util.http.ServiceUtil;
import com.mongodb.DuplicateKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


import java.util.logging.Level;

@RestController
public class RecommendationServiceImpl implements RecommendationService {

  private static final Logger LOG = LoggerFactory.getLogger(RecommendationServiceImpl.class);

  private final ServiceUtil serviceUtil;
  private final RecommendationMapper mapper;
  private final RecommendationRepository repository;

  @Autowired
  public RecommendationServiceImpl(ServiceUtil serviceUtil,RecommendationMapper mapper,RecommendationRepository repository) {
    this.serviceUtil = serviceUtil;
    this.mapper=mapper;
    this.repository=repository;
  }

  @Override
  public Flux<Recommendation> getRecommendations(int productId) {

    if (productId < 1) {
      throw new InvalidInputException("Invalid productId: " + productId);
    }
    LOG.info("Will get recommendations for product with id={}", productId);

    return repository.findByProductId(productId)
            .log(LOG.getName(), Level.FINE)
            .map(mapper::entityToApi)
            .map(this::setServiceAddress);


  }

  @Override
  public Mono<Recommendation> createRecommendation(Recommendation body) {
    if (body.getProductId() < 1) {
      throw new InvalidInputException("Invalid productId: " + body.getProductId());
    }
    RecommendationEntity entity = mapper.apiToEntity(body);

    return repository.save(entity)
            .log(LOG.getName(),Level.FINE)
            .onErrorMap(DuplicateKeyException.class,
                    ex -> new InvalidInputException("Duplicate key, Product Id: " + body.getProductId() + ", Recommendation Id:" + body.getRecommendationId()))
            .map(e -> mapper.entityToApi(e));
  }

  @Override
  public Mono<Void> deleteRecommendation(int productId) {
    if (productId < 1) {
      throw new InvalidInputException("Invalid productId: " + productId);
    }

    LOG.debug("deleteRecommendations: tries to delete recommendations for the product with productId: {}", productId);
    return repository.deleteAll(repository.findByProductId(productId));
  }


  private Recommendation setServiceAddress(Recommendation e) {
    e.setServiceAddress(serviceUtil.getServiceAddress());
    return e;
  }
}
