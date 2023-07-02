package com.fowlart.azure_store_sync;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Configuration
@EnableScheduling
public class CroneCheckConfig implements SchedulingConfigurer {

    final Environment env;

    public CroneCheckConfig(Environment env) {
        this.env = env;
    }

    public Executor taskExecutor() {
        return Executors.newScheduledThreadPool(100);
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {

        taskRegistrar.setScheduler(this.taskExecutor());

        taskRegistrar.addTriggerTask(() -> {


            var containerName = env.getProperty("azure.storage.container.name");
            var connectionString = env.getProperty("azure.storage.connection.string");
            var localFolderPath = env.getProperty("local.folder.path");

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(Objects.requireNonNull(connectionString)).buildClient();
            BlobContainerClient containerClient = blobServiceClient.createBlobContainerIfNotExists(containerName);

            // list all items in container
            var filesInBlob = containerClient.listBlobs().stream().map(BlobItem::getName).collect(Collectors.toSet());

            // set up local DIR, sync files between local DIR and blob
            var localFolder = new File(localFolderPath);

            if (!localFolder.exists()) {
                System.out.println("Attempt to create a folder for synchronization. Folder was created: " + localFolder.mkdirs());
            }

            var localFiles = Arrays.stream(localFolder.listFiles()).map(File::getName).collect(Collectors.toSet());

            var existInBlobButNotInLocal = filesInBlob.stream().filter(bf -> !localFiles.contains(bf)).collect(Collectors.toSet());

            var existInLocalButNotInBlob = localFiles.stream().filter(lf -> !filesInBlob.contains(lf)).collect(Collectors.toSet());

            if ("PUSH".equalsIgnoreCase(env.getProperty("spring.main.mode"))) {

                // if we have some files locally that need to be downloaded
                if (!existInLocalButNotInBlob.isEmpty()) {
                    existInLocalButNotInBlob.forEach(fname -> {
                        BlobClient blobClient = containerClient.getBlobClient(fname);
                        System.out.println("uploading file " + blobClient.getContainerName() + "/" + blobClient.getBlobName());
                        blobClient.uploadFromFile(localFolderPath + "/" + fname);
                    });
                }

                // delete redundancy in BLOB
                existInBlobButNotInLocal.forEach(fname -> {
                    BlobClient blobClient = containerClient.getBlobClient(fname);
                    blobClient.deleteIfExists();
                    System.out.println("deleting file with name NAME, result: RESULT".replaceAll("NAME", fname).replaceAll("RESULT", Boolean.toString(blobClient.deleteIfExists())));
                });
            }


            if ("FETCH".equalsIgnoreCase(env.getProperty("spring.main.mode"))){
                // if we have some files in blob that need to be downloaded
                if (!existInBlobButNotInLocal.isEmpty()) {

                    existInBlobButNotInLocal.forEach(fname -> {
                        BlobClient blobClient = containerClient.getBlobClient(fname);
                        System.out.println("downloading file " + blobClient.getContainerName() + "/" + blobClient.getBlobName());
                        blobClient.downloadToFile(localFolderPath + "/" + fname);
                    });
                }
                System.exit(0);
            }

            // Todo: create last accessed logging function
            if ("INVENTORY".equalsIgnoreCase(env.getProperty("spring.main.mode"))) {
                filesInBlob.forEach(blob->{
                    BlobClient blobClient = containerClient.getBlobClient(blob);
                    var props = blobClient.getProperties();
                    System.out.println("name: "+blob);
                    System.out.println("size: " + props.getBlobSize());
                    System.out.println("access tier: " + props.getAccessTier());
                    System.out.println("blob type: " + props.getBlobType().toString());
                    System.out.println("last accessed: " + props.getLastAccessedTime()+"\n");
                });
                System.exit(0);
            }

        }, triggerContext -> Instant.now().plusSeconds(Integer.parseInt(Objects.requireNonNull(env.getProperty("time.tick")))));
    }
}
