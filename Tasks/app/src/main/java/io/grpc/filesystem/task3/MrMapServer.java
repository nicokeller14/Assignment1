
/*
 * gRPC server node to accept calls from the clients and serve based on the method that has been requested
 * To build the project:
 * ./gradlew init
 * ./gradlew build
 * Run the MrMapServer:
 * ./gradlew run -PchooseMain=io.grpc.filesystem.task3.MrMapServer --args="50551"
 */

package io.grpc.filesystem.task3;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.ReduceInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

public class MrMapServer {

    private Server server;

    private void start(int port) throws IOException {
        server = ServerBuilder.forPort(port).addService(new MrMapServerImpl()).build().start();
        System.out.println("Listening on: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("Terminating the server at port: " + port);
                try {
                    server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }
        });
    }

    static class MrMapServerImpl extends AssignJobGrpc.AssignJobImplBase {
        MapReduce mr = new MapReduce();

        @Override
        public StreamObserver<MapInput> map(StreamObserver<MapOutput> responseObserver) {
            return new StreamObserver<MapInput>() {
                @Override
                public void onNext(MapInput input) {
                    // Execute map function
                    try {
                        mr.map(input.getInputfilepath());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }


                    // Send result back to the client
                    MapOutput output = MapOutput.newBuilder().setJobstatus(2).build();
                    responseObserver.onNext(output);
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Error occurred: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }


    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final MrMapServer mrServer = new MrMapServer();
        for (String i : args) {

            mrServer.start(Integer.parseInt(i));

        }
        mrServer.server.awaitTermination();
    }

}
