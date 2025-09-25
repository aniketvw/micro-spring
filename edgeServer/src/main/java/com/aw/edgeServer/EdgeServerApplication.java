package com.aw.edgeServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;

@SpringBootApplication
public class EdgeServerApplication {

	@Bean
	@LoadBalanced
	public WebClient.Builder webClientBalanced(){
		return  WebClient.builder();
	}

	public static void main(String[] args) {
		SpringApplication.run(EdgeServerApplication.class, args);
	}

}
