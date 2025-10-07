package com.aw.micro.product_composite_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@EnableWebSecurity
public class SecurityConfig {



    @Bean
    SecurityWebFilterChain springSecurityFilterChain(ServerHttpSecurity httpSecurity){

        httpSecurity
                .authorizeExchange(
                        exchange->exchange
                                .pathMatchers("/openapi/**", "/webjars/**", "/actuator/**").permitAll()
                                .pathMatchers(HttpMethod.POST, "/product-composite/**").hasAuthority("SCOPE_product:write")
                                .pathMatchers(HttpMethod.DELETE, "/product-composite/**").hasAuthority("SCOPE_product:write")
                                .pathMatchers(HttpMethod.GET, "/product-composite/**").hasAuthority("SCOPE_product:read")
                                .anyExchange().authenticated()
                )
                .oauth2ResourceServer(oauth2->oauth2.jwt(Customizer.withDefaults()));
        return httpSecurity.build();

    }

}
