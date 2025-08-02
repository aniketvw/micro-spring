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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.web.bind.annotation.RestController;


import java.util.List;

@RestController
public class ReviewServiceImpl implements ReviewService {

  private static final Logger LOG = LoggerFactory.getLogger(ReviewServiceImpl.class);

  private final ServiceUtil serviceUtil;
  private final ReviewMapper mapper;
  private final ReviewRepository repository;

  @Autowired
  public ReviewServiceImpl(ServiceUtil serviceUtil,ReviewMapper mapper,ReviewRepository repository) {
    this.serviceUtil = serviceUtil;
    this.mapper=mapper;
    this.repository=repository;
  }

  @Override
  public List<Review> getReviews(int productId) {

    if (productId < 1) {
      throw new InvalidInputException("Invalid productId: " + productId);
    }

    List<ReviewEntity> entityList = repository.findByProductId(productId);
    List<Review> reviewList = mapper.entityListToApiList(entityList);
    reviewList.forEach(e -> e.setServiceAddress(serviceUtil.getServiceAddress()));

    LOG.debug("getReviews: response size: {}", reviewList.size());

    return reviewList;
  }

  @Override
  public Review createReview(Review body) {
  try{
    ReviewEntity reviewEntity = mapper.apiToEntity(body);
    ReviewEntity newEntity = repository.save(reviewEntity);
    LOG.debug("createReview: created a review entity: {}/{}", body.getProductId(), body.getReviewId());
    return mapper.entityToApi(newEntity);
  }catch (DataIntegrityViolationException dive){
    throw new InvalidInputException("Duplicate key, Product Id: " + body.getProductId() + ", Review Id:" + body.getReviewId());
  }

  }

  @Override
  public void deleteReviews(int productId) {
    LOG.debug("deleteReviews: tries to delete reviews for the product with productId: {}", productId);
    repository.deleteAll(repository.findByProductId(productId));
  }
}
