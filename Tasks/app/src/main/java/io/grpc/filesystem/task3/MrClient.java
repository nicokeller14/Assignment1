package io.grpc.filesystem.task3;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import com.task3.proto.*;
import io.grpc.filesystem.task2.*;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MrClient {
   private static final Logger logger = Logger.getLogger(MrClient.class.getName());
   private final Map<String, Integer> jobStatus = new HashMap<>();
   private CountDownLatch finishLatch;

   public void requestMap(String ip, int port, String inputfilepath, String outputfilepath) throws InterruptedException {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
      try {
         AssignJobGrpc.AssignJobStub stub = AssignJobGrpc.newStub(channel);
         finishLatch = new CountDownLatch(1);
         StreamObserver<MapInput> requestObserver = stub.map(new StreamObserver<MapOutput>() {
            @Override
            public void onNext(MapOutput output) {
               jobStatus.put(inputfilepath, output.getJobstatus());
               logger.info("Received MapOutput with job status: " + output.getJobstatus());
            }

            @Override
            public void onError(Throwable t) {
               logger.log(Level.SEVERE, "Error occurred during mapping", t);
            }

            @Override
            public void onCompleted() {
               channel.shutdownNow();
               finishLatch.countDown();
            }
         });

         for (String job : jobStatus.keySet()) {
            requestObserver.onNext(MapInput.newBuilder().setInputfilepath(job).build());
            logger.info("Sending map request for job: " + job);
         }

         requestObserver.onCompleted();
         channel.awaitTermination(5, TimeUnit.SECONDS);
      } finally {
         channel.shutdown();
      }
   }

   public int requestReduce(String ip, int port, String inputfilepath, String outputfilepath) throws IOException {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
      try {
         AssignJobGrpc.AssignJobBlockingStub stub = AssignJobGrpc.newBlockingStub(channel);
         ReduceInput input = ReduceInput.newBuilder()
                 .setInputfilepath(inputfilepath)
                 .setOutputfilepath(outputfilepath)
                 .build();
         ReduceOutput output = stub.reduce(input);
         logger.info("Received ReduceOutput with job status: " + output.getJobstatus());

         if (output.getJobstatus() == 2) {
            MapReduce.reduce(inputfilepath, outputfilepath);
         }

         return output.getJobstatus();
      } finally {
         channel.shutdown();
      }
   }


   private static List<File> getChunkFiles(String chunkPath) {
      File dir = new File(chunkPath);
      File[] directoryListing = dir.listFiles();
      return directoryListing != null ? Arrays.asList(directoryListing) : new ArrayList<>();
   }

   public static void main(String[] args) throws Exception {
      String ip = args[0];
      int mapport = Integer.parseInt(args[1]);
      int reduceport = Integer.parseInt(args[2]);
      String inputfilepath = args[3];
      String outputfilepath = args[4];

      MrClient client = new MrClient();
      MapReduce mr = new MapReduce();
      String chunkpath = mr.makeChunks(inputfilepath);

      List<File> chunks = getChunkFiles(chunkpath);
      chunks.forEach(chunk -> client.jobStatus.put(chunk.getPath(), 2));

      client.requestMap(ip, mapport, inputfilepath, outputfilepath);
      client.finishLatch.await();
      System.out.println("Job statuses: " + client.jobStatus);


      Set<Integer> values = new HashSet<>(client.jobStatus.values());
      if (values.size() == 1 && client.jobStatus.containsValue(2)) {
         int response = client.requestReduce(ip, reduceport, chunkpath, outputfilepath);

         if (response == 2) {
            System.out.println("Reduce task completed!");
         } else {
            System.out.println("Try again! " + response);
         }
      }
   }
}

