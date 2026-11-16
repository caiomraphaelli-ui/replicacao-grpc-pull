package br.utfpr.sd.raft;

import br.utfpr.sd.raft.grpc.Entry;

/** Uma entrada do log de operações: (índice, termo, operação). */
public class LogEntry {
    public long index;
    public long term;
    public String type;   // "PUT" ou "NOOP"
    public String key;
    public String value;

    public LogEntry() {}

    public LogEntry(long index, long term, String type, String key, String value) {
        this.index = index;
        this.term = term;
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public Entry toProto() {
        return Entry.newBuilder().setIndex(index).setTerm(term).setType(type)
                .setKey(key == null ? "" : key).setValue(value == null ? "" : value).build();
    }

    public static LogEntry fromProto(Entry e) {
        return new LogEntry(e.getIndex(), e.getTerm(), e.getType(), e.getKey(), e.getValue());
    }

    @Override
    public String toString() {
        if ("NOOP".equals(type)) return "[" + index + "|t" + term + " NOOP]";
        return "[" + index + "|t" + term + " " + key + "=" + value + "]";
    }
}
