package com.aw.micro.review_service.services;

import com.aw.micro.api.core.review.Review;
import com.aw.micro.review_service.persistence.ReviewEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

import java.util.List;

@Mapper
public interface ReviewMapper {

    @Mappings({
            @Mapping(target = "serviceAddress",ignore = true)
    })
    Review entityToApi(ReviewEntity entity);

    @Mappings({
            @Mapping(target = "id", ignore = true),
            @Mapping(target = "version", ignore = true)
    })
    ReviewEntity apiToEntity(Review api);

    List<Review> entityListToApiList(List<ReviewEntity> entity);

    List<ReviewEntity> apiListToEntityList(List<Review> api);

}
