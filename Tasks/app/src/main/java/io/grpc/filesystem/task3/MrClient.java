/*
 * Client program to request for map and reduce functions from the Server
 * To build the project:
 * cd path/to/task3/folder
 * ./gradlew init
 * ./gradlew build
 * run the MrClient:
 * ./gradlew run -PchooseMain=io.grpc.filesystem.task3.MrClient --args="127.0.0.1 50551 50552 input/pigs.txt output/output-task3.txt"
 */

package io.grpc.filesystem.task3;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.ReduceInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

import java.io.*;
import java.nio.charset.Charset;

public class MrClient {
   private CountDownLatch finishLatch;

   private String currentJob = null;

   Map<String, Integer> jobStatus = new HashMap<String, Integer>();

   public MrClient() {
      finishLatch = new CountDownLatch(1);
   }

   public void awaitCompletion() throws InterruptedException {
      finishLatch.await();
   }

   public void requestMap(String ip, Integer portnumber, String inputfilepath, String outputfilepath) throws InterruptedException {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build();
      AssignJobGrpc.AssignJobStub stub = AssignJobGrpc.newStub(channel);

      StreamObserver<MapInput> requestObserver = stub.map(new StreamObserver<MapOutput>() {
         @Override
         public void onNext(MapOutput output) {
            jobStatus.put(currentJob, output.getJobstatus());
         }

         @Override
         public void onError(Throwable t) {
            System.err.println("Error occurred: " + t.getMessage());
         }

         @Override
         public void onCompleted() {
            channel.shutdownNow();
            finishLatch.countDown();}
      });

      for (String job : jobStatus.keySet()) {
         currentJob = job;
         requestObserver.onNext(MapInput.newBuilder().setInputfilepath(job).build());
      }

      requestObserver.onCompleted();
   }


   public int requestReduce(String ip, Integer portnumber, String inputfilepath, String outputfilepath) {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build();
      AssignJobGrpc.AssignJobBlockingStub stub = AssignJobGrpc.newBlockingStub(channel);

      ReduceInput input = ReduceInput.newBuilder().setInputfilepath(inputfilepath).setOutputfilepath(outputfilepath).build();
      ReduceOutput output = stub.reduce(input);

      channel.shutdownNow();
      return output.getJobstatus();
   }
   public static void main(String[] args) throws Exception {

      String ip = args[0];
      Integer mapport = Integer.parseInt(args[1]);
      Integer reduceport = Integer.parseInt(args[2]);
      String inputfilepath = args[3];
      String outputfilepath = args[4];
      String jobtype = null;
      MrClient client = new MrClient();
      int response = 0;

      MapReduce mr = new MapReduce();
      String chunkpath = mr.makeChunks(inputfilepath);
      Integer noofjobs = 0;
      File dir = new File(chunkpath);
      File[] directoyListing = dir.listFiles();
      if (directoyListing != null) {
         for (File f : directoyListing) {
            if (f.isFile()) {
               noofjobs += 1;
               client.jobStatus.put(f.getPath(), 1);

            }

         }
      }
      client.requestMap(ip, mapport, inputfilepath, outputfilepath);
      client.awaitCompletion();

      System.out.println("bye");

      Set<Integer> values = new HashSet<Integer>(client.jobStatus.values());
      if (values.size() == 1 && client.jobStatus.containsValue(2)) {

         response = client.requestReduce(ip, reduceport, chunkpath + "/map", outputfilepath);
         if (response == 2) {

            System.out.println("Reduce task completed!");

         } else {
            System.out.println("Try again! " + response);
         }

      }

   }

}
