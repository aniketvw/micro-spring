package com.aw.micro.product_composite_service.services;

import com.aw.micro.api.core.product.Product;
import com.aw.micro.api.core.product.ProductService;
import com.aw.micro.api.core.recommendation.Recommendation;
import com.aw.micro.api.core.recommendation.RecommendationService;
import com.aw.micro.api.core.review.Review;
import com.aw.micro.api.core.review.ReviewService;
import com.aw.micro.api.exceptions.InvalidInputException;
import com.aw.micro.api.exceptions.NotFoundException;
import com.aw.micro.util.http.HttpErrorInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.UNPROCESSABLE_ENTITY;

@Component
public class ProductCompositeIntegration implements ProductService, RecommendationService, ReviewService {

  private static final Logger LOG = LoggerFactory.getLogger(ProductCompositeIntegration.class);

  private final RestTemplate restTemplate;
  private final ObjectMapper mapper;

  private final String productServiceUrl;
  private final String recommendationServiceUrl;
  private final String reviewServiceUrl;

  @Autowired
  public ProductCompositeIntegration(
    RestTemplate restTemplate,
    ObjectMapper mapper,
    @Value("${app.product-service.host}") String productServiceHost,
    @Value("${app.product-service.port}") int productServicePort,
    @Value("${app.recommendation-service.host}") String recommendationServiceHost,
    @Value("${app.recommendation-service.port}") int recommendationServicePort,
    @Value("${app.review-service.host}") String reviewServiceHost,
    @Value("${app.review-service.port}") int reviewServicePort) {

    this.restTemplate = restTemplate;
    this.mapper = mapper;

    productServiceUrl = "http://" + productServiceHost + ":" + productServicePort + "/product";
    recommendationServiceUrl = "http://" + recommendationServiceHost + ":" + recommendationServicePort + "/recommendation";
    reviewServiceUrl = "http://" + reviewServiceHost + ":" + reviewServicePort + "/review";
  }
  @Override
  public Product getProduct(int productId) {

    try {
      String url = productServiceUrl + "/" + productId;
      LOG.debug("Will call the getProduct API on URL: {}", url);

      Product product = restTemplate.getForObject(url, Product.class);
      LOG.debug("Found a product with id: {}", product.getProductId());

      return product;

    } catch (HttpClientErrorException ex) {

        throw handleHttpClientException(ex);
    }
  }

  @Override
  public Product createProduct(Product body) {


    try{
      String url= productServiceUrl;
      LOG.debug("Will post a new product to URL: {}", url);

      Product product = restTemplate.postForObject(url, body, Product.class);
      LOG.debug("Created a product with id: {}", product.getProductId());

      return product;
    }catch (HttpClientErrorException ex){
      throw handleHttpClientException(ex);
    }


  }

  @Override
  public void deleteProduct(int productId) {
    try{
      String url=productServiceUrl + "/" +productId;
      LOG.debug("Will call the deleteProduct API on URL: {}", url);

      restTemplate.delete(url);

    }catch (HttpClientErrorException ex){
      throw handleHttpClientException(ex);
    }




  }

  private RuntimeException handleHttpClientException(HttpClientErrorException ex) {
      HttpStatusCode statusCode = ex.getStatusCode();
      if (statusCode.equals(NOT_FOUND)) {
          return new NotFoundException(getErrorMessage(ex));
      } else if (statusCode.equals(UNPROCESSABLE_ENTITY)) {
          return new InvalidInputException(getErrorMessage(ex));
      }
      LOG.warn("Got an unexpected HTTP error: {}, will rethrow it", ex.getStatusCode());
      LOG.warn("Error body: {}", ex.getResponseBodyAsString());
      return ex;
  }

  @Override
  public List<Recommendation> getRecommendations(int productId) {

    try {
      String url = recommendationServiceUrl + "?productId=" + productId;

      LOG.debug("Will call getRecommendations API on URL: {}", url);
      List<Recommendation> recommendations = restTemplate
        .exchange(url, GET, null, new ParameterizedTypeReference<List<Recommendation>>() {})
        .getBody();

      LOG.debug("Found {} recommendations for a product with id: {}", recommendations.size(), productId);
      return recommendations;

    } catch (Exception ex) {
      LOG.warn("Got an exception while requesting recommendations, return zero recommendations: {}", ex.getMessage());
      return new ArrayList<>();
    }
  }

  @Override
  public Recommendation createRecommendation(Recommendation body) {
    try{
      String url=recommendationServiceUrl;

      Recommendation recommendation = restTemplate.postForObject(url, body, Recommendation.class);

      return recommendation;

    }catch (HttpClientErrorException ex){
      throw handleHttpClientException(ex);
    }

  }
  @Override
  public void deleteRecommendation(int productId) {

    try {

      String url=recommendationServiceUrl + "?productId=" +productId;
      LOG.debug("Will call the deleteRecommendations API on URL: {}", url);
      restTemplate.delete(url);
    }catch (HttpClientErrorException ex){
      throw handleHttpClientException(ex);
    }


  }

  public List<Review> getReviews(int productId) {

    try {
      String url = reviewServiceUrl + "?productId=" + productId;

      LOG.debug("Will call getReviews API on URL: {}", url);
      List<Review> reviews = restTemplate
        .exchange(url, GET, null, new ParameterizedTypeReference<List<Review>>() {})
        .getBody();

      LOG.debug("Found {} reviews for a product with id: {}", reviews.size(), productId);
      return reviews;

    } catch (Exception ex) {
      LOG.warn("Got an exception while requesting reviews, return zero reviews: {}", ex.getMessage());
      return new ArrayList<>();
    }
  }

  @Override
  public Review createReview(Review body) {
    try{
      String url=reviewServiceUrl;
      Review review = restTemplate.postForObject(url, body, Review.class);
      return review;
    }catch (HttpClientErrorException ex) {
      throw handleHttpClientException(ex);
    }
  }

  @Override
  public void deleteReviews(int productId) {
    try {
      String url = reviewServiceUrl + "?productId=" + productId;
      LOG.debug("Will call the deleteReviews API on URL: {}", url);

      restTemplate.delete(url);

    } catch (HttpClientErrorException ex) {
      throw handleHttpClientException(ex);
    }
  }

  private String getErrorMessage(HttpClientErrorException ex) {
    try {
      return mapper.readValue(ex.getResponseBodyAsString(), HttpErrorInfo.class).getMessage();
    } catch (IOException ioex) {
      return ex.getMessage();
    }
  }
}
