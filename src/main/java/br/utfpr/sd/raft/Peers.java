package br.utfpr.sd.raft;

import br.utfpr.sd.raft.grpc.ReplicationServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;
import java.util.concurrent.TimeUnit;

/** Canais gRPC deste nó para os outros 3 nós do cluster. */
public class Peers {
    private final Map<Integer, ManagedChannel> channels = new TreeMap<>();

    public Peers(int selfId) {
        for (int id : Config.allIds()) {
            if (id == selfId) continue;
            channels.put(id, ManagedChannelBuilder.forAddress(Config.HOST, Config.port(id))
                    .usePlaintext().build());
        }
    }

    public Set<Integer> ids() {
        return channels.keySet();
    }

    public ReplicationServiceGrpc.ReplicationServiceBlockingStub stub(int id, long deadlineMs) {
        return ReplicationServiceGrpc.newBlockingStub(channels.get(id))
                .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
    }

    /** Após falha, força reconexão imediata (sem o backoff exponencial padrão do gRPC),
     *  para que um nó religado seja encontrado rapidamente. */
    public void onFailure(int id) {
        ManagedChannel ch = channels.get(id);
        if (ch != null) ch.resetConnectBackoff();
    }

    public void shutdown() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
