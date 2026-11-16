# Trab3 – Replicação de Dados com gRPC (modelo Pull, estilo KRaft/Raft)

Cluster de **4 nós** (mesmo código, papéis SEGUIDOR / CANDIDATO / LIDER) + **1 cliente**.
Toda a comunicação usa **gRPC**. Quórum = 3.

## Estrutura

```
src/main/proto/replicacao.proto   -> contrato gRPC (ReplicationService entre nós, ClientService p/ cliente)
src/main/java/br/utfpr/sd/raft/
  Config.java      -> portas, quórum, timeouts
  RaftNode.java    -> TODA a lógica: eleição, Fetch (pull), commit, truncamento, persistência
  Storage.java     -> persistência em disco (data/node-X/*.json)
  Peers.java       -> canais gRPC para os outros nós
  NodeMain.java    -> processo de um nó (servidor gRPC)
  ClientMain.java  -> cliente interativo
  ResetData.java   -> apaga a pasta data/ (recomeçar do zero)
.run/              -> configurações de execução prontas para o IntelliJ
.vscode/           -> launch.json com as mesmas configurações para o VS Code
mvnw.cmd / mvnw    -> Maven Wrapper: roda o Maven sem precisar instalá-lo
```

Portas: N1 = 50051, N2 = 50052, N3 = 50053, N4 = 50054 (localhost).

## Rodando no IntelliJ

Pré-requisitos: **JDK 17+** e internet (o Maven baixa gRPC/protobuf na 1ª vez).

1. **File → Open…** e selecione a pasta `replicacao-grpc-pull` (a que contém o `pom.xml`). Escolha *Open as Project* / *Trust Project*.
2. Confira o JDK: **File → Project Structure → Project → SDK = 17 ou superior**.
3. **Gere o código gRPC** (só na 1ª vez e sempre que alterar o `.proto`):
   abra a aba **Maven** (lateral direita) → `replicacao-grpc-pull` → **Lifecycle → compile** (duplo clique).
   Depois clique no ícone **Reload All Maven Projects** (🔄) da mesma aba.
   As classes geradas ficam em `target/generated-sources/protobuf` e o IntelliJ passa a reconhecê-las
   (se ainda aparecerem em vermelho: botão direito no `pom.xml` → *Maven → Generate Sources and Update Folders*).
   O IntelliJ usa o Maven embutido dele, então não precisa do `mvnw`.
4. No seletor de execução (canto superior direito) já aparecem as configurações da pasta `.run`:
   - **Cluster (4 nos)** → sobe N1, N2, N3 e N4 de uma vez (uma aba no painel *Run* para cada nó)
   - **No 1 … No 4** → sobe um nó individual (usar para religar um nó que você derrubou)
   - **Cliente** → console interativo
   - **Limpar dados** → apaga `data/` (rode com os nós parados para recomeçar do zero)
5. Rode **Cluster (4 nos)**, espere ~3 s (eleição) e rode **Cliente**. Digite os comandos no console da aba *Cliente*.

**Derrubar um nó** = clicar no quadrado vermelho (Stop) na aba dele no painel *Run*.
**Religar** = escolher *No X* no seletor e clicar em Run (ou o botão *Rerun* da aba).

> Se as configs não aparecerem: *Run → Edit Configurations → + → Application*,
> main class `br.utfpr.sd.raft.NodeMain`, Program arguments `1` (e repita para 2, 3, 4),
> Working directory = pasta do projeto. Cliente: main class `br.utfpr.sd.raft.ClientMain`.

## Rodando no VS Code

O projeto já traz `.vscode/launch.json` com as mesmas configurações do IntelliJ.

1. Instale a extensão **Extension Pack for Java** (Microsoft) — o VS Code sugere sozinho ao abrir a pasta. JDK 17+.
2. **File → Open Folder** → pasta `replicacao-grpc-pull`.
3. Gere o código gRPC: no terminal do VS Code (PowerShell), na pasta do projeto:
   ```
   .\mvnw.cmd compile
   ```
   Não precisa ter o Maven instalado: o `mvnw.cmd` baixa o Maven sozinho na 1ª vez
   (para `%USERPROFILE%\.m2\wrapper`). Só precisa do JDK 17+ (`java -version` no terminal deve funcionar).
   Se as classes `br.utfpr.sd.raft.grpc.*` continuarem em vermelho: `Ctrl+Shift+P` →
   **Java: Clean Java Language Server Workspace** → *Reload*.
4. Aba **Run and Debug** (`Ctrl+Shift+D`) → selecione **Cluster (4 nos)** → ▶. Cada nó abre num terminal.
5. Selecione **Cliente** → ▶ e digite os comandos no terminal dele.

Derrubar um nó = no painel de terminais, selecione o terminal do nó e clique na lixeira (ou `Ctrl+C`).
Religar = rodar **No X** de novo. **Limpar dados** apaga `data/` (com os nós parados).

### Pelo terminal (alternativa, sem IDE)

Windows (PowerShell) — as aspas nos `-D...` são obrigatórias no PowerShell:
```
.\mvnw.cmd compile
.\mvnw.cmd exec:java "-Dexec.mainClass=br.utfpr.sd.raft.NodeMain" "-Dexec.args=1"    (um terminal por nó: 1..4)
.\mvnw.cmd exec:java "-Dexec.mainClass=br.utfpr.sd.raft.ClientMain"
```
Linux/macOS: igual, trocando `.\mvnw.cmd` por `./mvnw`.

## Comandos do cliente

| comando | o que faz |
|---|---|
| `write <chave> <valor>` | grava; se cair num seguidor, recebe a indicação do líder e **redireciona** |
| `writeto <no> <chave> <valor>` | grava começando por um nó específico (para mostrar o redirecionamento) |
| `read <no> [chave]` | lê **somente dados comitados** de um nó |
| `readall [chave]` | lê de todos os nós lado a lado |
| `status` | papel, termo, líder, commitIndex, último índice, voto, réplicas ativas |
| `log <no>` | mostra o log do nó (`C` = comitada, `U` = não comitada) |
| `crashwrite <chave> <valor>` | cenário 4B: o líder grava só localmente e o processo cai |
| `pause` / `resume` | cenário 5: pausa/retoma o avanço do commitIndex no líder |

## Roteiro dos cenários obrigatórios

Comece do zero: pare tudo → **Limpar dados** → **Cluster (4 nos)** → **Cliente**.

**1. Escrita normal e leitura distribuída**
```
status
writeto 1 x 10        (se N1 não for líder, mostra o redirecionamento)
readall x             (x=10 em todos os nós)
```

**2. Eleição automática por queda do líder**
`status` → veja quem é o líder (ex.: N4) → **Stop** na aba do N4 → espere ~3 s → `status`.
Novo líder com **termo incrementado**. Nos consoles: `Election timeout! SEGUIDOR -> CANDIDATO`,
`Voto RECEBIDO`, `ELEITO LIDER do termo 2`. Religue o N4: ele volta como SEGUIDOR do novo termo.

**3. Bloqueio por perda de quórum**
Derrube 2 seguidores (Stop) → `write z 30` → `RECUSADO ... sem quorum: apenas 2 de 4 nos ativos`.
No console do líder: `Replica NX INATIVA` e `QUORUM PERDIDO`. Religue-os → `Quorum restabelecido`.

**4A. Réplica desatualizada**
Derrube 1 seguidor (ex.: N1) → `write a 1`, `write b 2` (comitam com 3 nós) → religue N1.
Console do N1: `FETCH recebeu 2 entrada(s) ...` e `APLICADO no banco`. `readall` confirma.

**4B. Truncamento por divergência**
```
crashwrite perdido 999      (líder N_A grava [idx|termo perdido=999] só no seu log e cai)
status                      (aguarde: novo líder N_B, termo+1)
write w 40                  (comita no novo termo)
```
Religue N_A. No console dele: `Termo X -> X+1`, `Lider atual: N_B`,
`TRUNCAMENTO DE LOG: removidas entradas nao comitadas [.. perdido=999]`, `FETCH recebeu ...`.
`log <N_A>` mostra o log igual ao do líder; `readall perdido` → não encontrado em nenhum nó.

**5. Isolamento de dados não comitados**
```
pause                 (líder para de avançar o commitIndex)
write segredo 77      (após 5 s: TIMEOUT - commit pausado)
log 1                 (U idx=..  segredo=77  -> a réplica JÁ recebeu via Fetch)
readall segredo       (nao encontrado em todos -> invisível)
resume
readall segredo       (segredo = 77 em todos)
```

## Como funciona (para a defesa)

- **Pull / FetchRequest** (`RaftNode.fetchLoop`, `handleFetch`): cada seguidor envia a cada 100 ms
  `FetchRequest{term, replica_id, last_log_index, last_log_term}`. O líder:
  1. registra `lastFetchAt[réplica] = agora` (heartbeat);
  2. verifica consistência: a entrada `last_log_index` existe no seu log com o mesmo termo?
     Se não → responde `DIVERGING` com `truncate_to`;
  3. se sim → `matchIndex[réplica] = last_log_index`, recalcula o commit e devolve as entradas seguintes + `commit_index`.
- **Commit por quórum** (`advanceCommit`): ordena `[último índice do líder, matchIndex das 3 réplicas]`
  e pega o 3º maior (presente em ≥ 3 nós). Só avança se a entrada é do termo atual (regra do Raft;
  por isso o líder grava uma entrada `NOOP` ao ser eleito).
- **Réplica efetiva o dado** só ao receber, num próximo Fetch, o `commit_index` atualizado → aplica no banco (`applyCommitted`).
- **Escrita do cliente** (`write`): recusa se não for líder (devolve `leader_id`) ou se `nós ativos < 3`;
  senão grava no log com `(termo atual, próximo índice)` e só responde OK quando `commitIndex >= índice`.
- **Monitoramento** (`monitorReplicas`): réplica sem Fetch há > 1000 ms = INATIVA.
- **Eleição** (`startElection`, `handleRequestVote`): timeout aleatório 1,5–3 s reiniciado a cada Fetch
  bem-sucedido; candidato faz `termo+1`, vota em si e pede votos. Voto concedido se termo ok, ainda não votou
  em outro neste termo e o log do candidato é tão ou mais atualizado (último termo maior, ou igual com índice ≥).
  Termo maior em qualquer mensagem → atualiza o termo e vira SEGUIDOR (`becomeFollower`).
  O vencedor avisa os demais com `BeginQuorumEpoch` (como no KRaft).
- **Truncamento** (`truncateLog`): a réplica apaga as entradas a partir do ponto indicado pelo líder
  (nunca abaixo do commitIndex) e volta a buscar.
- **Persistência** (`Storage`): `state.json` (termo + votedFor), `log.json` (log), `db.json` (dados comitados),
  gravados com arquivo temporário + rename atômico.
