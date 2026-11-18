package br.utfpr.sd.raft;

import br.utfpr.sd.raft.grpc.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Lógica de um nó do cluster. O mesmo código assume os papéis SEGUIDOR, CANDIDATO e LIDER.
 */
public class RaftNode {

    public enum Role { SEGUIDOR, CANDIDATO, LIDER }

    private final int id;
    private final Storage storage;
    private final Peers peers;
    private final Random random = new Random();

    // ---------- estado persistente ----------
    private long currentTerm;
    private int votedFor;
    private final List<LogEntry> log;           // log.get(i-1) é a entrada de índice i
    private final Map<String, String> db;       // banco final (somente dados comitados)
    private long lastApplied;

    // ---------- estado volátil ----------
    private long commitIndex;
    private Role role = Role.SEGUIDOR;
    private int leaderId = -1;
    private long electionDeadline;
    private boolean leaderUnreachableLogged = false;
    private int discoverCursor = 0;
    private Set<Integer> votes = new HashSet<>();

    // ---------- estado do líder ----------
    private final Map<Integer, Long> matchIndex = new HashMap<>();   // último índice confirmado por réplica
    private final Map<Integer, Long> lastFetchAt = new HashMap<>();  // instante do último FetchRequest
    private final Set<Integer> activeReplicas = new TreeSet<>();
    private boolean quorumLostLogged = false;
    private long leaderSince = 0;
    private final Map<Long, CompletableFuture<Boolean>> pendingWrites = new HashMap<>();

    // ---------- depuração / cenários ----------
    private boolean commitPaused = false;
    private volatile boolean frozen = false;   // simulação de crash
    private long lastStatusPrint = 0;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, daemon("ticker"));
    private final ExecutorService rpcPool = Executors.newCachedThreadPool(daemon("rpc"));

    public RaftNode(int id, Storage storage, Peers peers) {
        this.id = id;
        this.storage = storage;
        this.peers = peers;

        Storage.State st = storage.loadState();
        this.currentTerm = st.currentTerm;
        this.votedFor = st.votedFor;
        this.log = storage.loadLog();
        Storage.Db d = storage.loadDb();
        this.db = d.data;
        this.lastApplied = d.lastApplied;
        // Tudo que já foi aplicado no banco estava comitado.
        this.commitIndex = lastApplied;
        resetElectionTimer();

        info("Estado carregado do disco (" + storage.dir() + "): termo=" + currentTerm
                + ", votouEm=" + fmtNode(votedFor) + ", entradasNoLog=" + log.size()
                + ", lastApplied=" + lastApplied + ", banco=" + db);
        info("Iniciando como SEGUIDOR.");
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::tick, 50, 50, TimeUnit.MILLISECONDS);
        Thread fetcher = new Thread(this::fetchLoop, "fetcher");
        fetcher.setDaemon(true);
        fetcher.start();
    }

    // =====================================================================
    // Utilidades do log
    // =====================================================================

    private long lastIndex() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).index;
    }

    private long termAt(long index) {
        if (index <= 0 || index > lastIndex()) return 0;
        return log.get((int) index - 1).term;
    }

    private LogEntry entryAt(long index) {
        return log.get((int) index - 1);
    }

    private void persistState() {
        storage.saveState(currentTerm, votedFor);
    }

    private void persistLog() {
        storage.saveLog(log);
    }

    private void resetElectionTimer() {
        long timeout = Config.ELECTION_TIMEOUT_MIN_MS
                + random.nextInt((int) (Config.ELECTION_TIMEOUT_MAX_MS - Config.ELECTION_TIMEOUT_MIN_MS));
        electionDeadline = System.currentTimeMillis() + timeout;
    }

    // =====================================================================
    // Laço periódico: election timeout, monitoramento de réplicas, status
    // =====================================================================

    private void tick() {
        try {
            boolean startElection = false;
            synchronized (this) {
                long now = System.currentTimeMillis();
                if (frozen) return;
                if (role != Role.LIDER && now >= electionDeadline) {
                    startElection = true;
                }
                if (role == Role.LIDER) {
                    monitorReplicas(now);
                }
                if (now - lastStatusPrint >= Config.STATUS_PRINT_MS) {
                    lastStatusPrint = now;
                    info("STATUS " + statusLine());
                }
            }
            if (startElection) startElection();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /** Líder: marca réplicas como ativas/inativas pelo instante do último FetchRequest. */
    private void monitorReplicas(long now) {
        for (int p : peers.ids()) {
            Long last = lastFetchAt.get(p);
            boolean active = last != null && now - last <= Config.HEARTBEAT_TIMEOUT_MS;
            if (active && activeReplicas.add(p)) {
                info("Replica N" + p + " ATIVA (FetchRequest recebido).");
            } else if (!active && activeReplicas.remove(p)) {
                info("Replica N" + p + " INATIVA (sem FetchRequest ha mais de "
                        + Config.HEARTBEAT_TIMEOUT_MS + "ms).");
            }
        }
        int alive = activeReplicas.size() + 1; // + o próprio líder
        boolean graceOver = now - leaderSince > Config.HEARTBEAT_TIMEOUT_MS; // réplicas ainda não buscaram
        if (alive < Config.QUORUM && !quorumLostLogged && graceOver) {
            quorumLostLogged = true;
            info("!!! QUORUM PERDIDO: apenas " + alive + " de " + Config.N + " nos ativos (minimo "
                    + Config.QUORUM + "). Novas escritas serao RECUSADAS.");
        } else if (alive >= Config.QUORUM && quorumLostLogged) {
            quorumLostLogged = false;
            info("Quorum restabelecido: " + alive + " nos ativos. Escritas liberadas.");
        }
    }

    private int activeCount() {
        long now = System.currentTimeMillis();
        int c = 1;
        for (int p : peers.ids()) {
            Long last = lastFetchAt.get(p);
            if (last != null && now - last <= Config.HEARTBEAT_TIMEOUT_MS) c++;
        }
        return c;
    }

    // =====================================================================
    // Transições de papel
    // =====================================================================

    /** Atualização obrigatória de termo / volta a seguidor. Chamar com o lock. */
    private void becomeFollower(long newTerm, String reason) {
        boolean wasLeader = role == Role.LIDER;
        Role old = role;
        if (newTerm > currentTerm) {
            info("Termo " + currentTerm + " -> " + newTerm + " (" + reason + ")");
            currentTerm = newTerm;
            votedFor = -1;
            persistState();
            leaderId = -1;
        }
        role = Role.SEGUIDOR;
        if (old != Role.SEGUIDOR) {
            info(">>> " + old + " -> SEGUIDOR (" + reason + ")");
        }
        if (wasLeader) {
            if (leaderId == id) leaderId = -1;
            activeReplicas.clear();
            failPendingWrites("no deixou de ser lider");
        }
        resetElectionTimer();
    }

    private void startElection() {
        VoteRequest req;
        long term;
        synchronized (this) {
            if (role == Role.LIDER || frozen) return;
            Role old = role;
            role = Role.CANDIDATO;
            currentTerm++;
            votedFor = id;
            persistState();
            leaderId = -1;
            votes = new HashSet<>();
            votes.add(id);
            resetElectionTimer();
            term = currentTerm;
            info(">>> Election timeout! " + old + " -> CANDIDATO. Termo incrementado para " + term
                    + ". Votou em si mesmo. Pedindo votos (log: idx=" + lastIndex()
                    + ", termo=" + termAt(lastIndex()) + ")");
            req = VoteRequest.newBuilder().setTerm(term).setCandidateId(id)
                    .setLastLogIndex(lastIndex()).setLastLogTerm(termAt(lastIndex())).build();
        }
        for (int p : peers.ids()) {
            rpcPool.submit(() -> {
                try {
                    VoteResponse r = peers.stub(p, 500).requestVote(req);
                    onVoteResponse(p, term, r);
                } catch (StatusRuntimeException e) {
                    peers.onFailure(p);
                }
            });
        }
    }

    private synchronized void onVoteResponse(int from, long electionTerm, VoteResponse r) {
        if (r.getTerm() > currentTerm) {
            becomeFollower(r.getTerm(), "resposta de voto com termo maior de N" + from);
            return;
        }
        if (role != Role.CANDIDATO || currentTerm != electionTerm) return;
        if (r.getVoteGranted()) {
            votes.add(from);
            info("Voto RECEBIDO de N" + from + " (" + votes.size() + "/" + Config.QUORUM + " necessarios)");
            if (votes.size() >= Config.QUORUM) becomeLeader();
        } else {
            info("Voto NEGADO por N" + from);
        }
    }

    /** Chamar com o lock. */
    private void becomeLeader() {
        role = Role.LIDER;
        leaderId = id;
        matchIndex.clear();
        lastFetchAt.clear();
        activeReplicas.clear();
        quorumLostLogged = false;
        leaderSince = System.currentTimeMillis();
        info("##### ELEITO LIDER do termo " + currentTerm + " com votos de " + fmtSet(votes) + " #####");
        // Entrada NOOP do novo termo: permite comitar entradas de termos anteriores (regra do Raft).
        LogEntry noop = new LogEntry(lastIndex() + 1, currentTerm, "NOOP", "", "");
        log.add(noop);
        persistLog();
        info("Gravou entrada NOOP " + noop + " para iniciar o termo.");
        long term = currentTerm;
        BeginQuorumEpochRequest req = BeginQuorumEpochRequest.newBuilder().setTerm(term).setLeaderId(id).build();
        for (int p : peers.ids()) {
            rpcPool.submit(() -> {
                try {
                    BeginQuorumEpochResponse r = peers.stub(p, 500).beginQuorumEpoch(req);
                    synchronized (RaftNode.this) {
                        if (r.getTerm() > currentTerm) becomeFollower(r.getTerm(), "termo maior de N" + p);
                    }
                } catch (StatusRuntimeException e) {
                    peers.onFailure(p);
                }
            });
        }
    }

    private void setLeader(int newLeader) {
        if (leaderId != newLeader) {
            info("Lider atual: N" + newLeader + " (termo " + currentTerm + ")");
        }
        leaderId = newLeader;
        leaderUnreachableLogged = false;
    }

    // =====================================================================
    // Handlers dos RPCs entre nós
    // =====================================================================

    /** RequestVote: voto único por termo + candidato precisa ter log tão ou mais atualizado. */
    public synchronized VoteResponse handleRequestVote(VoteRequest req) {
        if (frozen) throw Status.UNAVAILABLE.asRuntimeException();
        int c = req.getCandidateId();
        if (req.getTerm() > currentTerm) {
            becomeFollower(req.getTerm(), "RequestVote com termo maior de N" + c);
        }
        long myLastIdx = lastIndex();
        long myLastTerm = termAt(myLastIdx);
        boolean upToDate = req.getLastLogTerm() > myLastTerm
                || (req.getLastLogTerm() == myLastTerm && req.getLastLogIndex() >= myLastIdx);

        boolean granted = false;
        String why;
        if (req.getTerm() < currentTerm) {
            why = "termo do candidato (" + req.getTerm() + ") menor que o meu (" + currentTerm + ")";
        } else if (votedFor != -1 && votedFor != c) {
            why = "ja votei em N" + votedFor + " no termo " + currentTerm;
        } else if (!upToDate) {
            why = "log do candidato desatualizado (candidato: idx=" + req.getLastLogIndex() + ",termo="
                    + req.getLastLogTerm() + " / meu: idx=" + myLastIdx + ",termo=" + myLastTerm + ")";
        } else {
            votedFor = c;
            persistState();
            granted = true;
            resetElectionTimer();
            why = "ok";
        }
        info("RequestVote de N" + c + " (termo " + req.getTerm() + "): "
                + (granted ? "VOTO CONCEDIDO" : "VOTO NEGADO - " + why));
        return VoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(granted).build();
    }

    public synchronized BeginQuorumEpochResponse handleBeginQuorumEpoch(BeginQuorumEpochRequest req) {
        if (frozen) throw Status.UNAVAILABLE.asRuntimeException();
        if (req.getTerm() >= currentTerm) {
            if (req.getTerm() > currentTerm || role != Role.SEGUIDOR) {
                becomeFollower(req.getTerm(), "N" + req.getLeaderId() + " anunciou ser lider do termo " + req.getTerm());
            }
            setLeader(req.getLeaderId());
            resetElectionTimer();
        }
        return BeginQuorumEpochResponse.newBuilder().setTerm(currentTerm).build();
    }

    /** Líder atende o FetchRequest de uma réplica. */
    public synchronized FetchResponse handleFetch(FetchRequest req) {
        if (frozen) throw Status.UNAVAILABLE.asRuntimeException();
        FetchResponse.Builder b = FetchResponse.newBuilder();
        int r = req.getReplicaId();

        if (req.getTerm() > currentTerm) {
            becomeFollower(req.getTerm(), "FetchRequest com termo maior de N" + r);
        }
        if (role != Role.LIDER) {
            return b.setStatus(FetchResponse.Status.NOT_LEADER).setTerm(currentTerm)
                    .setLeaderId(leaderId).build();
        }
        if (req.getTerm() < currentTerm) {
            return b.setStatus(FetchResponse.Status.STALE_TERM).setTerm(currentTerm).setLeaderId(id).build();
        }

        // heartbeat: registra o instante do último Fetch desta réplica
        lastFetchAt.put(r, System.currentTimeMillis());

        long li = req.getLastLogIndex();
        long lt = req.getLastLogTerm();
        // Verificação de consistência: a última entrada da réplica existe no meu log com o mesmo termo?
        if (li > lastIndex() || termAt(li) != lt) {
            long t = Math.min(li, lastIndex());
            while (t > 0 && termAt(t) > lt) t--;
            info("DIVERGENCIA no log de N" + r + " (ultima entrada idx=" + li + ", termo=" + lt
                    + "; no meu log idx " + li + " tem termo " + (li <= lastIndex() ? termAt(li) : "inexistente")
                    + "). Instruindo truncamento a partir do idx " + (t + 1) + ".");
            return b.setStatus(FetchResponse.Status.DIVERGING).setTerm(currentTerm).setLeaderId(id)
                    .setTruncateTo(t).setCommitIndex(commitIndex).build();
        }

        // Log consistente: a réplica confirma que possui tudo até li.
        Long prev = matchIndex.put(r, li);
        if (prev == null || prev != li) {
            advanceCommit();
        }

        long from = li + 1;
        long to = Math.min(lastIndex(), li + Config.MAX_BATCH);
        for (long i = from; i <= to; i++) b.addEntries(entryAt(i).toProto());
        return b.setStatus(FetchResponse.Status.OK).setTerm(currentTerm).setLeaderId(id)
                .setCommitIndex(commitIndex).build();
    }

    /** Líder: avança o commitIndex para o maior índice presente em pelo menos QUORUM nós. */
    private void advanceCommit() {
        if (role != Role.LIDER || commitPaused) return;
        List<Long> idx = new ArrayList<>();
        idx.add(lastIndex()); // o próprio líder
        for (int p : peers.ids()) idx.add(matchIndex.getOrDefault(p, 0L));
        idx.sort(Comparator.reverseOrder());
        long n = idx.get(Config.QUORUM - 1);  // 3º maior: está em >= 3 nós
        // Só comita diretamente entradas do termo atual (regra de segurança do Raft).
        if (n > commitIndex && termAt(n) == currentTerm) {
            info("COMMIT avancou " + commitIndex + " -> " + n + " (indices confirmados [lider + replicas]: "
                    + "N" + id + "=" + lastIndex() + matchIndexStr() + ")");
            commitIndex = n;
            applyCommitted();
            completePendingWrites();
        }
    }

    private String matchIndexStr() {
        StringBuilder sb = new StringBuilder();
        for (int p : peers.ids()) sb.append(", N").append(p).append("=").append(matchIndex.getOrDefault(p, 0L));
        return sb.toString();
    }

    /** Aplica no banco final (db) as entradas com índice <= commitIndex. */
    private void applyCommitted() {
        boolean changed = false;
        while (lastApplied < commitIndex) {
            LogEntry e = entryAt(lastApplied + 1);
            if ("PUT".equals(e.type)) {
                db.put(e.key, e.value);
                info("APLICADO no banco: " + e + "  (agora visivel para leitura)");
            }
            lastApplied++;
            changed = true;
        }
        if (changed) storage.saveDb(lastApplied, db);
    }

    // =====================================================================
    // Réplica: laço de Fetch ativo
    // =====================================================================

    private void fetchLoop() {
        while (true) {
            try {
                fetchOnce();
                Thread.sleep(Config.FETCH_INTERVAL_MS);
            } catch (InterruptedException e) {
                return;
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private void fetchOnce() {
        int target;
        FetchRequest req;
        synchronized (this) {
            if (role != Role.SEGUIDOR || frozen) return;
            target = leaderId > 0 && leaderId != id ? leaderId : nextDiscoverTarget();
            req = FetchRequest.newBuilder().setTerm(currentTerm).setReplicaId(id)
                    .setLastLogIndex(lastIndex()).setLastLogTerm(termAt(lastIndex())).build();
        }
        FetchResponse resp;
        try {
            resp = peers.stub(target, Config.RPC_DEADLINE_MS).fetch(req);
        } catch (StatusRuntimeException e) {
            peers.onFailure(target);
            synchronized (this) {
                if (leaderId == target) {
                    if (!leaderUnreachableLogged) {
                        info("Falha no FetchRequest ao lider N" + target + " (" + e.getStatus().getCode()
                                + "). Aguardando election timeout...");
                        leaderUnreachableLogged = true;
                    }
                    leaderId = -1;
                }
            }
            return;
        }
        handleFetchResponse(target, resp);
    }

    /** Sem líder conhecido: pergunta aos outros nós em round-robin (eles indicam o líder). */
    private int nextDiscoverTarget() {
        List<Integer> ids = new ArrayList<>(peers.ids());
        int t = ids.get(discoverCursor % ids.size());
        discoverCursor++;
        return t;
    }

    private synchronized void handleFetchResponse(int from, FetchResponse resp) {
        if (resp.getTerm() > currentTerm) {
            becomeFollower(resp.getTerm(), "FetchResponse com termo maior de N" + from);
        }
        if (role != Role.SEGUIDOR) return;

        switch (resp.getStatus()) {
            case NOT_LEADER -> {
                if (resp.getLeaderId() > 0 && resp.getLeaderId() != id && resp.getTerm() >= currentTerm) {
                    setLeader(resp.getLeaderId());
                }
            }
            case STALE_TERM -> { /* termo já atualizado acima; próximo Fetch vai com o termo certo */ }
            case DIVERGING -> {
                if (resp.getTerm() != currentTerm) return;
                setLeader(from);
                resetElectionTimer();
                truncateLog(resp.getTruncateTo());
            }
            case OK -> {
                if (resp.getTerm() != currentTerm) return;
                setLeader(from);
                resetElectionTimer();   // FetchRequest bem-sucedido reinicia o temporizador
                appendFromLeader(resp.getEntriesList());
                long newCommit = Math.min(resp.getCommitIndex(), lastIndex());
                if (newCommit > commitIndex) {
                    info("Lider informou commitIndex=" + resp.getCommitIndex() + " -> commit local "
                            + commitIndex + " -> " + newCommit);
                    commitIndex = newCommit;
                    applyCommitted();
                }
            }
            default -> { }
        }
    }

    private void appendFromLeader(List<Entry> entries) {
        List<LogEntry> added = new ArrayList<>();
        for (Entry e : entries) {
            if (e.getIndex() == lastIndex() + 1) {
                LogEntry le = LogEntry.fromProto(e);
                log.add(le);
                added.add(le);
            }
        }
        if (!added.isEmpty()) {
            persistLog();
            info("FETCH recebeu " + added.size() + " entrada(s) " + shortList(added)
                    + " gravadas no LOG (ainda NAO comitadas / invisiveis para leitura)");
        }
    }

    /** Remove do log local todas as entradas com índice > target. */
    private void truncateLog(long target) {
        long last = lastIndex();
        if (target >= last) target = last - 1;          // garante progresso
        if (target < commitIndex) target = commitIndex; // nunca remove dado comitado
        if (target >= last) return;
        List<LogEntry> tail = log.subList((int) target, log.size());
        List<LogEntry> removed = new ArrayList<>(tail);
        tail.clear();
        persistLog();
        info("TRUNCAMENTO DE LOG: removidas entradas nao comitadas " + shortList(removed)
                + ". Log agora termina no idx " + lastIndex() + ". Ressincronizando com o lider...");
    }

    // =====================================================================
    // Operações do cliente
    // =====================================================================

    public WriteResponse write(String key, String value) {
        CompletableFuture<Boolean> fut;
        LogEntry e;
        synchronized (this) {
            WriteResponse.Builder b = WriteResponse.newBuilder().setTerm(currentTerm);
            if (role != Role.LIDER || frozen) {
                info("Escrita recebida, mas nao sou lider -> indicando lider N" + leaderId);
                return b.setResult(WriteResponse.Result.NOT_LEADER).setLeaderId(leaderId)
                        .setMessage("N" + id + " e " + role + "; lider atual: " + fmtNode(leaderId)).build();
            }
            int alive = activeCount();
            if (alive < Config.QUORUM) {
                String msg = "sem quorum: apenas " + alive + " de " + Config.N + " nos ativos (minimo "
                        + Config.QUORUM + ")";
                info("Escrita " + key + "=" + value + " RECUSADA: " + msg);
                return b.setResult(WriteResponse.Result.NO_QUORUM).setLeaderId(id).setMessage(msg).build();
            }
            e = new LogEntry(lastIndex() + 1, currentTerm, "PUT", key, value);
            log.add(e);
            persistLog();
            info("Escrita do cliente gravada no log local: " + e + " - aguardando quorum...");
            fut = new CompletableFuture<>();
            pendingWrites.put(e.index, fut);
        }
        try {
            boolean ok = fut.get(Config.WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (ok) {
                info("Confirmando ao cliente: " + e + " COMITADA.");
                return WriteResponse.newBuilder().setResult(WriteResponse.Result.COMMITTED).setLeaderId(id)
                        .setIndex(e.index).setTerm(e.term).setMessage("comitado").build();
            }
            return WriteResponse.newBuilder().setResult(WriteResponse.Result.FAILED).setLeaderId(leaderId)
                    .setIndex(e.index).setTerm(e.term).setMessage("lider perdeu a lideranca antes do commit").build();
        } catch (TimeoutException te) {
            boolean paused;
            synchronized (this) {
                pendingWrites.remove(e.index);
                paused = commitPaused;
            }
            return WriteResponse.newBuilder().setResult(WriteResponse.Result.TIMEOUT).setLeaderId(id)
                    .setIndex(e.index).setTerm(e.term)
                    .setMessage("entrada " + e + " nao foi comitada em " + Config.WRITE_TIMEOUT_MS + "ms"
                            + (paused ? " (commit PAUSADO no lider: a entrada esta no log, mas ainda nao comitada)" : ""))
                    .build();
        } catch (Exception ex) {
            return WriteResponse.newBuilder().setResult(WriteResponse.Result.FAILED)
                    .setMessage(ex.toString()).build();
        }
    }

    private void completePendingWrites() {
        Iterator<Map.Entry<Long, CompletableFuture<Boolean>>> it = pendingWrites.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, CompletableFuture<Boolean>> en = it.next();
            if (en.getKey() <= commitIndex) {
                en.getValue().complete(true);
                it.remove();
            }
        }
    }

    private void failPendingWrites(String why) {
        pendingWrites.values().forEach(f -> f.complete(false));
        pendingWrites.clear();
    }

    /** Leitura eventual: responde SOMENTE com dados comitados (banco final). */
    public synchronized ReadResponse read(String key) {
        ReadResponse.Builder b = ReadResponse.newBuilder().setNodeId(id).setRole(role.name())
                .setCommitIndex(commitIndex);
        if (key == null || key.isEmpty()) {
            b.putAllData(db);
        } else if (db.containsKey(key)) {
            b.setFound(true).setValue(db.get(key));
        }
        return b.build();
    }

    public synchronized StatusResponse status() {
        if (frozen) throw Status.UNAVAILABLE.asRuntimeException();
        return StatusResponse.newBuilder().setNodeId(id).setRole(role.name()).setTerm(currentTerm)
                .setLeaderId(leaderId).setCommitIndex(commitIndex).setLastLogIndex(lastIndex())
                .setVotedFor(votedFor).addAllActiveReplicas(role == Role.LIDER ? activeReplicas : Set.of())
                .setCommitPaused(commitPaused).build();
    }

    public synchronized LogResponse dumpLog() {
        LogResponse.Builder b = LogResponse.newBuilder().setNodeId(id).setCommitIndex(commitIndex);
        for (LogEntry e : log) b.addEntries(e.toProto());
        return b.build();
    }

    /** Cenário 4B: o líder grava localmente e "cai" antes de qualquer réplica buscar a entrada. */
    public WriteResponse debugWriteAndCrash(String key, String value) {
        synchronized (this) {
            if (role != Role.LIDER) {
                return WriteResponse.newBuilder().setResult(WriteResponse.Result.NOT_LEADER)
                        .setLeaderId(leaderId).setMessage("N" + id + " nao e lider").build();
            }
            frozen = true; // a partir daqui nenhum Fetch é atendido
            LogEntry e = new LogEntry(lastIndex() + 1, currentTerm, "PUT", key, value);
            log.add(e);
            persistLog();
            info("SIMULACAO DE CRASH: entrada " + e + " gravada SOMENTE no log local (nao replicada). "
                    + "Encerrando o processo...");
            scheduler.schedule(() -> Runtime.getRuntime().halt(1), 300, TimeUnit.MILLISECONDS);
            return WriteResponse.newBuilder().setResult(WriteResponse.Result.FAILED).setLeaderId(id)
                    .setIndex(e.index).setTerm(e.term)
                    .setMessage("entrada " + e + " gravada so no lider N" + id + ", que caiu em seguida").build();
        }
    }

    /** Cenário 5: pausa o avanço do commitIndex no líder. */
    public synchronized StatusResponse setCommitPaused(boolean paused) {
        commitPaused = paused;
        info(paused ? "COMMIT PAUSADO (cenario 5): replicas continuam buscando entradas, mas o commitIndex nao avanca."
                : "COMMIT RETOMADO.");
        if (!paused) advanceCommit();
        return status();
    }

    // =====================================================================
    // Saída no console
    // =====================================================================

    private static final DateTimeFormatter TF = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private String statusLine() {
        String s = "papel=" + role + " | termo=" + currentTerm + " | commitIndex=" + commitIndex
                + " | ultimoIndiceLog=" + lastIndex() + " | lider=" + fmtNode(leaderId)
                + " | votouEm=" + fmtNode(votedFor);
        if (role == Role.LIDER) s += " | replicasAtivas=" + fmtSet(activeReplicas);
        if (commitPaused) s += " | COMMIT PAUSADO";
        return s;
    }

    private void info(String msg) {
        System.out.println(LocalTime.now().format(TF) + " [N" + id + " | " + role + " | termo=" + currentTerm
                + " | commit=" + commitIndex + "] " + msg);
    }

    private static String fmtNode(int n) {
        return n > 0 ? "N" + n : "-";
    }

    private static String fmtSet(Collection<Integer> s) {
        return s.stream().sorted().map(i -> "N" + i).collect(Collectors.joining(",", "[", "]"));
    }

    private static String shortList(List<LogEntry> l) {
        return l.stream().map(LogEntry::toString).collect(Collectors.joining(" "));
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    public void shutdown() {
        scheduler.shutdownNow();
        rpcPool.shutdownNow();
    }
}
