package br.utfpr.sd.raft;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Persistência em disco (pasta data/node-X):
 *   state.json -> termo atual e voto concedido (voted_for)
 *   log.json   -> log de operações (índice, termo, operação)
 *   db.json    -> banco final (apenas dados comitados) + lastApplied
 *
 * Toda gravação é feita em arquivo temporário + rename atômico,
 * então derrubar o processo no meio não corrompe os arquivos.
 */
public class Storage {

    public static class State {
        public long currentTerm = 0;
        public int votedFor = -1;
    }

    public static class Db {
        public long lastApplied = 0;
        public Map<String, String> data = new TreeMap<>();
    }

    private final Path dir;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public Storage(Path dir) {
        this.dir = dir;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path dir() {
        return dir;
    }

    public State loadState() {
        State s = read("state.json", State.class);
        return s == null ? new State() : s;
    }

    public void saveState(long currentTerm, int votedFor) {
        State s = new State();
        s.currentTerm = currentTerm;
        s.votedFor = votedFor;
        write("state.json", gson.toJson(s));
    }

    public List<LogEntry> loadLog() {
        Path p = dir.resolve("log.json");
        if (!Files.exists(p)) return new ArrayList<>();
        try {
            List<LogEntry> l = gson.fromJson(Files.readString(p, StandardCharsets.UTF_8),
                    new TypeToken<List<LogEntry>>() {}.getType());
            return l == null ? new ArrayList<>() : new ArrayList<>(l);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void saveLog(List<LogEntry> log) {
        write("log.json", gson.toJson(log));
    }

    public Db loadDb() {
        Db d = read("db.json", Db.class);
        if (d == null) return new Db();
        d.data = new TreeMap<>(d.data == null ? Map.of() : d.data);
        return d;
    }

    public void saveDb(long lastApplied, Map<String, String> data) {
        Db d = new Db();
        d.lastApplied = lastApplied;
        d.data = new TreeMap<>(data);
        write("db.json", gson.toJson(d));
    }

    private <T> T read(String name, Class<T> cls) {
        Path p = dir.resolve(name);
        if (!Files.exists(p)) return null;
        try {
            return gson.fromJson(Files.readString(p, StandardCharsets.UTF_8), cls);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void write(String name, String content) {
        Path target = dir.resolve(name);
        Path tmp = dir.resolve(name + ".tmp");
        try {
            Files.writeString(tmp, content, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.SYNC);
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFail) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
