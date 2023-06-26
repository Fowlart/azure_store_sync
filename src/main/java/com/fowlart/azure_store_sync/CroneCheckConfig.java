package com.fowlart.azure_store_sync;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
@EnableScheduling
public class CroneCheckConfig implements SchedulingConfigurer {

    @Autowired
    Environment env;

    public Executor taskExecutor() {
        return Executors.newScheduledThreadPool(100);
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setScheduler(this.taskExecutor());
        taskRegistrar.addTriggerTask(() -> {

            // 1 -  list all items in container
            var containerName = env.getProperty("azure.storage.container.name");
            var connectionString = env.getProperty("azure.storage.connection.string");

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(Objects.requireNonNull(connectionString))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient
                    .createBlobContainerIfNotExists(containerName);

            var itemList = containerClient.listBlobs();

            itemList.forEach(it -> System.out.println(it.getName()));

            // todo: 2 - set up local DIR, sync files between local DIR and blob
            var localFolder = new File("/Users/artur/IdeaProjects/azure_store_sync/sync");
            if (!localFolder.exists()) localFolder.mkdirs();

            }, triggerContext ->
                Instant.now().plusSeconds(Integer.parseInt(Objects.requireNonNull(env.getProperty("time.tick")))));
    }
}
