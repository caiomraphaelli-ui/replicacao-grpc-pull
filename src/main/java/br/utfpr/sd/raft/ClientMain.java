package br.utfpr.sd.raft;

import br.utfpr.sd.raft.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Cliente interativo (linha de comando). Digite "help" para ver os comandos.
 */
public class ClientMain {

    private final Map<Integer, ManagedChannel> channels = new TreeMap<>();
    private int knownLeader = -1;

    public ClientMain() {
        for (int id : Config.allIds()) {
            channels.put(id, ManagedChannelBuilder.forAddress(Config.HOST, Config.port(id)).usePlaintext().build());
        }
    }

    private ClientServiceGrpc.ClientServiceBlockingStub stub(int id, long ms) {
        ManagedChannel ch = channels.get(id);
        ch.resetConnectBackoff();
        return ClientServiceGrpc.newBlockingStub(ch).withDeadlineAfter(ms, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        ClientMain c = new ClientMain();
        if (args.length > 0) {           // permite rodar um comando direto: ClientMain write x 1
            c.exec(String.join(" ", args));
            return;
        }
        c.help();
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print("\ncliente> ");
            System.out.flush();
            if (!sc.hasNextLine()) break;
            String line = sc.nextLine().trim();
            if (line.equals("exit") || line.equals("sair")) break;
            if (!line.isEmpty()) c.exec(line);
        }
        System.exit(0);
    }

    void help() {
        System.out.println("""
                ================= CLIENTE - Replicacao gRPC (modelo Pull) =================
                 write <chave> <valor>            grava (descobre o lider e redireciona)
                 writeto <no> <chave> <valor>     grava comecando por um no especifico (demonstra redirecionamento)
                 read <no> [chave]                le dados COMITADOS de um no (sem chave = tudo)
                 readall [chave]                  le de todos os nos lado a lado
                 status                           papel, termo, commitIndex, lider de cada no
                 log <no>                         mostra o log do no (C = comitada, U = nao comitada)
                 crashwrite <chave> <valor>       [cenario 4B] lider grava so localmente e cai
                 pause | resume                   [cenario 5] pausa/retoma o avanco do commit no lider
                 help | exit
                ===========================================================================""");
    }

    void exec(String line) {
        String[] p = line.split("\\s+");
        try {
            switch (p[0].toLowerCase()) {
                case "write", "w" -> {
                    need(p, 3);
                    write(knownLeader > 0 ? knownLeader : 1, p[1], joinFrom(p, 2));
                }
                case "writeto" -> {
                    need(p, 4);
                    write(Integer.parseInt(p[1]), p[2], joinFrom(p, 3));
                }
                case "read", "r" -> {
                    need(p, 2);
                    read(Integer.parseInt(p[1]), p.length > 2 ? p[2] : "");
                }
                case "readall", "ra" -> {
                    for (int id : Config.allIds()) read(id, p.length > 1 ? p[1] : "");
                }
                case "status", "s" -> status();
                case "log" -> {
                    need(p, 2);
                    log(Integer.parseInt(p[1]));
                }
                case "crashwrite" -> {
                    need(p, 3);
                    crashWrite(p[1], joinFrom(p, 2));
                }
                case "pause" -> pause(true);
                case "resume" -> pause(false);
                case "help", "h", "?" -> help();
                default -> System.out.println("Comando desconhecido. Digite help.");
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Uso incorreto. Digite help.");
        }
    }

    private static void need(String[] p, int n) {
        if (p.length < n) throw new IllegalArgumentException();
    }

    private static String joinFrom(String[] p, int i) {
        return String.join(" ", Arrays.copyOfRange(p, i, p.length));
    }

    private static int next(int id) {
        return id % Config.N + 1;
    }

    // ------------------------------------------------------------------

    void write(int start, String key, String value) {
        WriteRequest req = WriteRequest.newBuilder().setKey(key).setValue(value).build();
        int target = start;
        for (int attempt = 0; attempt < 10; attempt++) {
            System.out.println("-> enviando escrita " + key + "=" + value + " para N" + target);
            WriteResponse r;
            try {
                r = stub(target, Config.WRITE_TIMEOUT_MS + 2000).write(req);
            } catch (StatusRuntimeException e) {
                System.out.println("   N" + target + " indisponivel (" + e.getStatus().getCode() + "), tentando outro no...");
                if (knownLeader == target) knownLeader = -1;
                target = next(target);
                sleep(300);
                continue;
            }
            switch (r.getResult()) {
                case COMMITTED -> {
                    knownLeader = target;
                    System.out.println("OK: " + key + "=" + value + " COMITADO no indice " + r.getIndex()
                            + " (termo " + r.getTerm() + ") pelo lider N" + target);
                    return;
                }
                case NOT_LEADER -> {
                    if (r.getLeaderId() > 0 && r.getLeaderId() != target) {
                        System.out.println("   N" + target + " NAO e lider (" + r.getMessage()
                                + ") -> REDIRECIONANDO para N" + r.getLeaderId());
                        target = r.getLeaderId();
                    } else {
                        System.out.println("   N" + target + " nao e lider e nao conhece o lider (eleicao em andamento?)."
                                + " Tentando outro no...");
                        target = next(target);
                        sleep(500);
                    }
                }
                case NO_QUORUM -> {
                    knownLeader = target;
                    System.out.println("RECUSADO pelo lider N" + target + ": " + r.getMessage());
                    return;
                }
                case TIMEOUT -> {
                    System.out.println("TIMEOUT no lider N" + target + ": " + r.getMessage());
                    return;
                }
                default -> {
                    System.out.println("FALHA: " + r.getMessage());
                    return;
                }
            }
        }
        System.out.println("Nao foi possivel encontrar um lider.");
    }

    void read(int id, String key) {
        try {
            ReadResponse r = stub(id, 1000).read(ReadRequest.newBuilder().setKey(key).build());
            String head = "N" + id + " (" + r.getRole() + ", commit=" + r.getCommitIndex() + "): ";
            if (key.isEmpty()) {
                System.out.println(head + (r.getDataMap().isEmpty() ? "{}" : new TreeMap<>(r.getDataMap())));
            } else {
                System.out.println(head + key + " = " + (r.getFound() ? r.getValue() : "(nao encontrado)"));
            }
        } catch (StatusRuntimeException e) {
            System.out.println("N" + id + ": OFFLINE");
        }
    }

    void status() {
        System.out.printf("%-4s %-10s %-6s %-7s %-8s %-9s %-8s %s%n",
                "No", "Papel", "Termo", "Lider", "Commit", "UltIdx", "VotouEm", "Replicas ativas (lider)");
        for (int id : Config.allIds()) {
            try {
                StatusResponse s = stub(id, 1000).status(Empty.getDefaultInstance());
                System.out.printf("%-4s %-10s %-6d %-7s %-8d %-9d %-8s %s%n", "N" + id, s.getRole(), s.getTerm(),
                        s.getLeaderId() > 0 ? "N" + s.getLeaderId() : "-", s.getCommitIndex(),
                        s.getLastLogIndex(), s.getVotedFor() > 0 ? "N" + s.getVotedFor() : "-",
                        s.getRole().equals("LIDER") ? s.getActiveReplicasList()
                                + (s.getCommitPaused() ? " COMMIT PAUSADO" : "") : "");
                if (s.getRole().equals("LIDER")) knownLeader = id;
            } catch (StatusRuntimeException e) {
                System.out.printf("%-4s %s%n", "N" + id, "OFFLINE");
            }
        }
    }

    void log(int id) {
        try {
            LogResponse r = stub(id, 1000).dumpLog(Empty.getDefaultInstance());
            System.out.println("Log de N" + id + " (commitIndex=" + r.getCommitIndex() + "):");
            if (r.getEntriesCount() == 0) System.out.println("  (vazio)");
            for (Entry e : r.getEntriesList()) {
                String op = e.getType().equals("NOOP") ? "NOOP" : e.getKey() + "=" + e.getValue();
                System.out.printf("  %s idx=%-3d termo=%-3d %s%n",
                        e.getIndex() <= r.getCommitIndex() ? "C" : "U", e.getIndex(), e.getTerm(), op);
            }
        } catch (StatusRuntimeException e) {
            System.out.println("N" + id + ": OFFLINE");
        }
    }

    private int findLeader() {
        if (knownLeader > 0) {
            try {
                if (stub(knownLeader, 1000).status(Empty.getDefaultInstance()).getRole().equals("LIDER"))
                    return knownLeader;
            } catch (StatusRuntimeException ignored) { }
        }
        for (int id : Config.allIds()) {
            try {
                if (stub(id, 1000).status(Empty.getDefaultInstance()).getRole().equals("LIDER")) {
                    knownLeader = id;
                    return id;
                }
            } catch (StatusRuntimeException ignored) { }
        }
        return -1;
    }

    void crashWrite(String key, String value) {
        int l = findLeader();
        if (l < 0) {
            System.out.println("Nenhum lider encontrado.");
            return;
        }
        try {
            WriteResponse r = stub(l, 2000).debugWriteAndCrash(
                    WriteRequest.newBuilder().setKey(key).setValue(value).build());
            System.out.println("crashwrite em N" + l + ": " + r.getMessage());
            knownLeader = -1;
        } catch (StatusRuntimeException e) {
            System.out.println("Erro: " + e.getStatus());
        }
    }

    void pause(boolean paused) {
        int l = findLeader();
        if (l < 0) {
            System.out.println("Nenhum lider encontrado.");
            return;
        }
        StatusResponse s = stub(l, 1000).debugPauseCommit(PauseRequest.newBuilder().setPaused(paused).build());
        System.out.println("Lider N" + l + ": commit " + (s.getCommitPaused() ? "PAUSADO" : "RETOMADO")
                + " (commitIndex=" + s.getCommitIndex() + ", ultimoIdx=" + s.getLastLogIndex() + ")");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) { }
    }
}
