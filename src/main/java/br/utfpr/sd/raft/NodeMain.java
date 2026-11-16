package br.utfpr.sd.raft;

import br.utfpr.sd.raft.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.nio.file.Path;
import java.util.function.Supplier;

/**
 * Processo de um nó do cluster.  Uso:  NodeMain <id>   (id = 1, 2, 3 ou 4)
 */
public class NodeMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: NodeMain <id 1..4>");
            System.exit(1);
        }
        int id = Integer.parseInt(args[0]);
        if (!Config.allIds().contains(id)) {
            System.err.println("id deve ser 1..4");
            System.exit(1);
        }

        Storage storage = new Storage(Path.of("data", "node-" + id));
        Peers peers = new Peers(id);
        RaftNode node = new RaftNode(id, storage, peers);

        Server server = ServerBuilder.forPort(Config.port(id))
                .addService(new ReplicationServiceImpl(node))
                .addService(new ClientServiceImpl(node))
                .build()
                .start();

        System.out.println("=== No N" + id + " escutando gRPC em " + Config.HOST + ":" + Config.port(id)
                + " | cluster N=" + Config.N + ", quorum=" + Config.QUORUM + " ===");
        node.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            node.shutdown();
            peers.shutdown();
            server.shutdownNow();
        }));
        server.awaitTermination();
    }

    private static <T> void reply(StreamObserver<T> obs, Supplier<T> s) {
        try {
            obs.onNext(s.get());
            obs.onCompleted();
        } catch (StatusRuntimeException e) {
            obs.onError(e);
        } catch (Exception e) {
            obs.onError(io.grpc.Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
        }
    }

    /** RPCs entre os nós do cluster. */
    static class ReplicationServiceImpl extends ReplicationServiceGrpc.ReplicationServiceImplBase {
        private final RaftNode node;

        ReplicationServiceImpl(RaftNode node) {
            this.node = node;
        }

        @Override
        public void fetch(FetchRequest req, StreamObserver<FetchResponse> obs) {
            reply(obs, () -> node.handleFetch(req));
        }

        @Override
        public void requestVote(VoteRequest req, StreamObserver<VoteResponse> obs) {
            reply(obs, () -> node.handleRequestVote(req));
        }

        @Override
        public void beginQuorumEpoch(BeginQuorumEpochRequest req, StreamObserver<BeginQuorumEpochResponse> obs) {
            reply(obs, () -> node.handleBeginQuorumEpoch(req));
        }
    }

    /** RPCs usados pelo cliente. */
    static class ClientServiceImpl extends ClientServiceGrpc.ClientServiceImplBase {
        private final RaftNode node;

        ClientServiceImpl(RaftNode node) {
            this.node = node;
        }

        @Override
        public void write(WriteRequest req, StreamObserver<WriteResponse> obs) {
            reply(obs, () -> node.write(req.getKey(), req.getValue()));
        }

        @Override
        public void read(ReadRequest req, StreamObserver<ReadResponse> obs) {
            reply(obs, () -> node.read(req.getKey()));
        }

        @Override
        public void status(Empty req, StreamObserver<StatusResponse> obs) {
            reply(obs, node::status);
        }

        @Override
        public void dumpLog(Empty req, StreamObserver<LogResponse> obs) {
            reply(obs, node::dumpLog);
        }

        @Override
        public void debugWriteAndCrash(WriteRequest req, StreamObserver<WriteResponse> obs) {
            reply(obs, () -> node.debugWriteAndCrash(req.getKey(), req.getValue()));
        }

        @Override
        public void debugPauseCommit(PauseRequest req, StreamObserver<StatusResponse> obs) {
            reply(obs, () -> node.setCommitPaused(req.getPaused()));
        }
    }
}
