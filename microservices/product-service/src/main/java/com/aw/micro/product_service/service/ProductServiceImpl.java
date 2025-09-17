package com.aw.micro.product_service.service;

import com.aw.micro.api.core.product.Product;
import com.aw.micro.api.core.product.ProductService;
import com.aw.micro.api.exceptions.InvalidInputException;
import com.aw.micro.api.exceptions.NotFoundException;
import com.aw.micro.product_service.persistence.ProductEntity;
import com.aw.micro.product_service.persistence.ProductRepository;
import com.aw.micro.util.http.ServiceUtil;
import com.mongodb.DuplicateKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.logging.Level;

@RestController
public class ProductServiceImpl implements ProductService {


    private static final Logger LOG = LoggerFactory.getLogger(ProductServiceImpl.class);

    private final ServiceUtil serviceUtil;

    private final ProductRepository repository;

    private final ProductMapper mapper;

    @Autowired
    public ProductServiceImpl(ServiceUtil serviceUtil, ProductMapper mapper, ProductRepository repository) {
        this.mapper = mapper;
        this.repository = repository;
        this.serviceUtil = serviceUtil;
    }

    @Override
    public Mono<Product>  createProduct(Product body) {
        if (body.getProductId() < 1) {
        throw new InvalidInputException("Invalid productId: " + body.getProductId());
    }
        ProductEntity productEntity = mapper.apiToEntity(body);

        return repository.save(productEntity)
                .log(LOG.getName(),Level.FINE)
                .onErrorMap(DuplicateKeyException.class,
                        ex->new InvalidInputException("\"Duplicate key, Product Id: "+ body.getProductId()))
                .map(mapper::entityToApi);

    }

    @Override
    public Mono<Product> getProduct(int productId) {
        LOG.debug("/product return the found product for productId={}", productId);

        if (productId < 1) {
            throw new InvalidInputException("Invalid productId: " + productId);
        }

       return  repository.findByProductId(productId)
               .switchIfEmpty(Mono.error(new NotFoundException("No product for id: "+productId)))
               .log(LOG.getName(), Level.FINE)
               .map(mapper::entityToApi)
               .map(this::setServiceAddress);
    }

    @Override
    public Mono<Void> deleteProduct(int productId){
        if (productId < 1) {
            throw new InvalidInputException("Invalid productId: " + productId);
        }

        LOG.debug("deleteProduct: tries to delete an entity with productId: {}", productId);

        return  repository.findByProductId(productId)
                .log(LOG.getName(),Level.FINE)
                .map(e->repository.delete(e)).flatMap(e->e);

    }

    private Product setServiceAddress(Product e) {
        e.setServiceAddress(serviceUtil.getServiceAddress());
        return e;
    }

}
