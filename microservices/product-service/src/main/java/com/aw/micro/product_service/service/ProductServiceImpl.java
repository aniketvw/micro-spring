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
    public Product createProduct(Product body) {
        try {
            ProductEntity productEntity = mapper.apiToEntity(body);
            ProductEntity savedEntity = repository.save(productEntity);
            LOG.debug("createProduct: entity created for productId: {}", body.getProductId());
            return mapper.entityToApi(savedEntity);
        } catch (DuplicateKeyException duplicateKeyException) {
            throw new InvalidInputException("Duplicate key, Product Id: " + body.getProductId());
        }

    }

    @Override
    public Product getProduct(int productId) {
        LOG.debug("/product return the found product for productId={}", productId);

        if (productId < 1) {
            throw new InvalidInputException("Invalid productId: " + productId);
        }

       ProductEntity entity=repository.findByProductId(productId)
               .orElseThrow(()->new NotFoundException("No product found for productId: " + productId));

        Product response=mapper.entityToApi(entity);
        response.setServiceAddress(serviceUtil.getServiceAddress());

        LOG.debug("getProduct: found productId: {}", response.getProductId());

        return response;
    }

    @Override
    public void deleteProduct(int productId){
        LOG.debug("deleteProduct: tries to delete an entity with productId: {}", productId);
        repository.findByProductId(productId).ifPresent(repository::delete);
    }
}
