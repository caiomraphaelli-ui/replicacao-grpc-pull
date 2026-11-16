package br.utfpr.sd.raft;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.stream.Stream;

/** Apaga a pasta data/ (estado persistido de todos os nós). Rode com os nós DESLIGADOS. */
public class ResetData {
    public static void main(String[] args) throws IOException {
        Path data = Path.of("data");
        if (!Files.exists(data)) {
            System.out.println("Nada para apagar (pasta data/ nao existe).");
            return;
        }
        try (Stream<Path> s = Files.walk(data)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        System.out.println("Pasta data/ apagada. Cluster vai iniciar do zero.");
    }
}
