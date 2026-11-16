package br.utfpr.sd.raft;

import java.util.List;

/** Parâmetros fixos do cluster. */
public final class Config {
    private Config() {}

    /** Número de nós do cluster. */
    public static final int N = 4;
    /** Quórum = maioria absoluta = floor(N/2) + 1 = 3. */
    public static final int QUORUM = N / 2 + 1;

    public static final String HOST = System.getProperty("raft.host", "localhost");

    /** Nó i escuta na porta 50050 + i (N1 = 50051 ... N4 = 50054). */
    public static int port(int id) {
        return 50050 + id;
    }

    public static List<Integer> allIds() {
        return List.of(1, 2, 3, 4);
    }

    /** Intervalo entre FetchRequests das réplicas (também é o heartbeat). */
    public static final long FETCH_INTERVAL_MS = 100;
    /** Election timeout aleatório em [MIN, MAX] — evita split vote. */
    public static final long ELECTION_TIMEOUT_MIN_MS = 1500;
    public static final long ELECTION_TIMEOUT_MAX_MS = 3000;
    /** Heartbeat Timeout do líder para cada réplica (sem Fetch nesse tempo = inativa). */
    public static final long HEARTBEAT_TIMEOUT_MS = 1000;
    /** Quanto tempo o líder espera o commit antes de responder TIMEOUT ao cliente. */
    public static final long WRITE_TIMEOUT_MS = 5000;
    /** Deadline das chamadas gRPC entre nós. */
    public static final long RPC_DEADLINE_MS = 300;
    /** Máximo de entradas enviadas por FetchResponse. */
    public static final int MAX_BATCH = 100;
    /** De quanto em quanto tempo o nó imprime uma linha de status. */
    public static final long STATUS_PRINT_MS = 5000;
}
