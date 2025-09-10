package com.aw.micro.product_composite_service;

import com.aw.micro.product_composite_service.services.ProductCompositeIntegration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.health.CompositeReactiveHealthContributor;
import org.springframework.boot.actuate.health.ReactiveHealthContributor;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.RestTemplate;

import java.util.LinkedHashMap;
import java.util.Map;

@SpringBootApplication
@ComponentScan("com.aw")
public class ProductCompositeServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProductCompositeServiceApplication.class, args);
	}

	@Autowired
	ProductCompositeIntegration integration;

	@Bean
	ReactiveHealthContributor coreServices() {

		final Map<String, ReactiveHealthIndicator> registry = new LinkedHashMap<>();

		registry.put("product", () -> integration.getProductHealth());
		registry.put("recommendation", () -> integration.getRecommendationHealth());
		registry.put("review", () -> integration.getReviewHealth());

		return CompositeReactiveHealthContributor.fromMap(registry);
	}

}
