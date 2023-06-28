package com.fowlart.azure_store_sync;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class AzureStoreSyncApplication {
	public static void main(String[] args) {
		System.out.println(Arrays.toString(args));
		SpringApplication.run(AzureStoreSyncApplication.class, args);
	}
}
