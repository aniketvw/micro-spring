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


import java.util.ArrayList;
import java.util.List;

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
  public List<Recommendation> getRecommendations(int productId) {

    if (productId < 1) {
      throw new InvalidInputException("Invalid productId: " + productId);
    }

    List<RecommendationEntity> entityList=repository.findByProductId(productId);
    List<Recommendation> list = mapper.entityListToApiList(entityList);
    list.forEach(e->e.setServiceAddress(serviceUtil.getServiceAddress()));

    LOG.debug("getRecommendations: response size: {}", list.size());


    return list;
  }

  @Override
  public Recommendation createRecommendation(Recommendation body) {
    try{
      RecommendationEntity recommendationEntity = mapper.apiToEntity(body);
      RecommendationEntity newEntity = repository.save(recommendationEntity);

      LOG.debug("createRecommendation: created a recommendation entity: {}/{}", body.getProductId(), body.getRecommendationId());
      return mapper.entityToApi(newEntity);
    }catch (DuplicateKeyException dke){
      throw new InvalidInputException("Duplicate key, Product Id: " + body.getProductId() + ", Recommendation Id:" + body.getRecommendationId());
    }
  }

  @Override
  public void deleteRecommendation(int productId) {
    LOG.debug("deleteRecommendations: tries to delete recommendations for the product with productId: {}", productId);
    repository.deleteAll(repository.findByProductId(productId));
  }
}
