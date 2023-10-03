package io.grpc.filesystem.task3;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.task3.proto.AssignJobGrpc;
import com.task3.proto.ReduceInput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

public class MrReduceServer {
    private static final Logger logger = Logger.getLogger(MrReduceServer.class.getName());
    private Server server;

    private void start(int port) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MrReduceServerImpl())
                .build()
                .start();
        logger.info("Listening on: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warning("Terminating the server at port: " + port);
            try {
                server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "Server termination interrupted", e);
            }
        }));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final MrReduceServer mrServer = new MrReduceServer();
        for (String i : args) {
            mrServer.start(Integer.parseInt(i));
        }
        mrServer.server.awaitTermination();
    }

    static class MrReduceServerImpl extends AssignJobGrpc.AssignJobImplBase {
        private final MapReduce mr = new MapReduce();

        @Override
        public void reduce(ReduceInput reduceInput, StreamObserver<ReduceOutput> responseObserver) {
            logger.info("Received ReduceInput for filepath: " + reduceInput.getInputfilepath());
            // Extract data from ReduceInput
            String inputPath = reduceInput.getInputfilepath();
            String outputFilePath = reduceInput.getOutputfilepath();

            try {
                mr.reduce(inputPath, outputFilePath);
                logger.info("Reduction completed for filepath: " + inputPath);

                ReduceOutput successOutput = ReduceOutput.newBuilder().setJobstatus(2).build();
                responseObserver.onNext(successOutput);
                responseObserver.onCompleted();

            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error during reduction", e);

                ReduceOutput errorOutput = ReduceOutput.newBuilder().setJobstatus(0).build(); // Changed to 0 assuming it denotes an error state
                responseObserver.onNext(errorOutput);
                responseObserver.onCompleted();
            }
        }
    }
}
