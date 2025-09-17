package com.aw.micro.recommendation_service.persistence;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface RecommendationRepository extends ReactiveCrudRepository<RecommendationEntity,String> {
    Flux<RecommendationEntity> findByProductId(int productId);
}
