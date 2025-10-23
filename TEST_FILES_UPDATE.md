# Aggiornamenti File di Test

Questo documento descrive le modifiche apportate ai file di test per supportare l'architettura multi-canale RPC.

## test/client.c

### Modifiche

**Linea 65**: Sostituito `rpc_router_init()` con `rpc_client_connect()`

```c
// PRIMA
if (rpc_router_init(&channel, router_ip, router_port, 4096) != 0) {

// DOPO
if (rpc_client_connect(&channel, router_ip, router_port, RPC_CHANNEL_INGRESS, 4096) != 0) {
```

### Motivazione

Il client esterno deve connettersi al canale **INGRESS** del router per inviare richieste di gestione DAG e submit di task.

### Utilizzo

```bash
# Connetti al router sulla porta INGRESS (6002)
./client 127.0.0.1 6002 "Test Message"
```

---

## test/router.c

### Modifiche

#### 1. Parametri da linea di comando (Linee 22-31)

```c
// PRIMA
if (argc < 3) {
    fprintf(stderr, "Usage: %s <router_id> <port>\n", argv[0]);
    fprintf(stderr, "Example: %s 1 5000\n", argv[0]);
    return 1;
}
node_id_t router_id = (node_id_t)atoi(argv[1]);
uint16_t port = (uint16_t)atoi(argv[2]);

// DOPO
if (argc < 5) {
    fprintf(stderr, "Usage: %s <router_id> <service_port> <data_port> <ingress_port>\n", argv[0]);
    fprintf(stderr, "Example: %s 1 6000 6001 6002\n", argv[0]);
    return 1;
}
node_id_t router_id = (node_id_t)atoi(argv[1]);
uint16_t service_port = (uint16_t)atoi(argv[2]);
uint16_t data_port = (uint16_t)atoi(argv[3]);
uint16_t ingress_port = (uint16_t)atoi(argv[4]);
```

#### 2. Inizializzazione router (Linea 49)

```c
// PRIMA
if (router_init(&g_router, router_id, port) != RESULT_OK) {

// DOPO
if (router_init(&g_router, router_id, service_port, data_port, ingress_port) != RESULT_OK) {
```

#### 3. Avvio server RPC (Linea 103)

```c
// PRIMA
rpc_worker_run(port, router_rpc_service_table);

// DOPO
rpc_router_run(service_port, data_port, ingress_port, router_rpc_service_table);
```

### Motivazione

Il router necessita di tre canali distinti:
- **SERVICE** (6000): Gestione worker (heartbeat, registrazione)
- **DATA** (6001): Comunicazione dati con worker
- **INGRESS** (6002): Richieste da client esterni

### Utilizzo

```bash
# Avvia router con tre porte separate
./router 1 6000 6001 6002
#        │ │    │    │
#        │ │    │    └─ INGRESS port (client requests)
#        │ │    └────── DATA port (worker data)
#        │ └─────────── SERVICE port (worker management)
#        └───────────── Router ID
```

---

## test/worker.c

### Modifiche

#### 1. Parametri da linea di comando (Linee 22-35)

```c
// PRIMA
if (argc < 3) {
    fprintf(stderr, "Usage: %s <worker_id> <port> [num_threads] [router_ip] [router_port]\n", argv[0]);
    fprintf(stderr, "Example: %s 100 6000 4 127.0.0.1 5000\n", argv[0]);
    return 1;
}
node_id_t worker_id = (node_id_t)atoi(argv[1]);
uint16_t port = (uint16_t)atoi(argv[2]);
size_t num_threads = (argc >= 4) ? (size_t)atoi(argv[3]) : 4;
const char *router_ip = (argc >= 5) ? argv[4] : NULL;
uint16_t router_port = (argc >= 6) ? (uint16_t)atoi(argv[5]) : 5000;

// DOPO
if (argc < 4) {
    fprintf(stderr, "Usage: %s <worker_id> <service_port> <data_port> [num_threads] [router_ip] [router_service_port] [router_data_port]\n", argv[0]);
    fprintf(stderr, "Example: %s 100 5000 5001 4 127.0.0.1 6000 6001\n", argv[0]);
    return 1;
}
node_id_t worker_id = (node_id_t)atoi(argv[1]);
uint16_t service_port = (uint16_t)atoi(argv[2]);
uint16_t data_port = (uint16_t)atoi(argv[3]);
size_t num_threads = (argc >= 5) ? (size_t)atoi(argv[4]) : 4;
const char *router_ip = (argc >= 6) ? argv[5] : NULL;
uint16_t router_service_port = (argc >= 7) ? (uint16_t)atoi(argv[6]) : 6000;
uint16_t router_data_port = (argc >= 8) ? (uint16_t)atoi(argv[7]) : 6001;
```

#### 2. Inizializzazione worker (Linea 58)

```c
// PRIMA
if (worker_init(&g_worker, worker_id, port, num_threads) != RESULT_OK) {

// DOPO
if (worker_init(&g_worker, worker_id, service_port, data_port, num_threads) != RESULT_OK) {
```

#### 3. Registrazione con router (Linea 74)

```c
// PRIMA
if (worker_register_with_router(&g_worker, router_ip, router_port) == RESULT_OK) {

// DOPO
if (worker_register_with_router(&g_worker, router_ip,
                               router_service_port, router_data_port) == RESULT_OK) {
```

#### 4. Avvio server RPC (Linea 92)

```c
// PRIMA
rpc_worker_run(port, worker_rpc_service_table);

// DOPO
rpc_worker_run(service_port, data_port, worker_rpc_service_table);
```

### Motivazione

Il worker necessita di due canali distinti:
- **SERVICE** (5000): Heartbeat e registrazione con router
- **DATA** (5001): Ricezione task e invio aggiornamenti esecuzione

Inoltre, deve conoscere le porte SERVICE e DATA del router per stabilire le connessioni corrette.

### Utilizzo

```bash
# Worker standalone (senza router)
./worker 100 5000 5001 4
#        │   │    │    │
#        │   │    │    └─ Numero thread esecutori
#        │   │    └────── DATA port
#        │   └─────────── SERVICE port
#        └───────────────── Worker ID

# Worker con registrazione al router
./worker 100 5000 5001 4 127.0.0.1 6000 6001
#        │   │    │    │ │         │    │
#        │   │    │    │ │         │    └─ Router DATA port
#        │   │    │    │ │         └────── Router SERVICE port
#        │   │    │    │ └──────────────── Router IP
#        │   │    │    └────────────────── Numero thread
#        │   │    └─────────────────────── Worker DATA port
#        │   └──────────────────────────── Worker SERVICE port
#        └──────────────────────────────── Worker ID
```

---

## Esempio di Deployment Completo

### Avvio componenti

```bash
# Terminal 1: Router
./router 1 6000 6001 6002

# Terminal 2: Worker 1
./worker 100 5000 5001 4 127.0.0.1 6000 6001

# Terminal 3: Worker 2
./worker 101 5010 5011 4 127.0.0.1 6000 6001

# Terminal 4: Client
./client 127.0.0.1 6002 "Hello from client"
```

### Flusso di comunicazione

1. **Worker → Router (SERVICE)**: Registrazione e heartbeat
   - Worker si connette a `127.0.0.1:6000` (Router SERVICE)
   - Invia `FUNC_ID_WORKER_REGISTRATION`

2. **Router → Worker (DATA)**: Assegnazione task
   - Router si connette a `worker_ip:5001` (Worker DATA)
   - Invia `FUNC_ID_EXECUTE_DAG`

3. **Worker → Router (DATA)**: Aggiornamenti esecuzione
   - Worker usa canale DATA esistente
   - Invia `FUNC_ID_EXECUTION_UPDATE`

4. **Client → Router (INGRESS)**: Richieste DAG
   - Client si connette a `127.0.0.1:6002` (Router INGRESS)
   - Invia `FUNC_ID_ADD_DAG`, `FUNC_ID_SUBMIT_TASK`, ecc.

---

## Schema Porte Consigliato

| Componente | Canale | Porta | Descrizione |
|-----------|--------|-------|-------------|
| Router | SERVICE | 6000 | Gestione worker |
| Router | DATA | 6001 | Comunicazione con worker |
| Router | INGRESS | 6002 | Richieste client |
| Worker 1 | SERVICE | 5000 | Heartbeat/registrazione |
| Worker 1 | DATA | 5001 | Esecuzione task |
| Worker 2 | SERVICE | 5010 | Heartbeat/registrazione |
| Worker 2 | DATA | 5011 | Esecuzione task |

---

## Note Importanti

1. **Backward Compatibility**: Le modifiche **non sono backward compatible**. I vecchi binari non funzioneranno.

2. **Firewall**: Assicurarsi che tutte le porte siano aperte nel firewall per le comunicazioni necessarie:
   - Router: 6000, 6001, 6002 (in ingresso)
   - Worker: 5000, 5001 (in ingresso)

3. **Testing**: Testare sempre la connettività tra componenti prima del deployment in produzione.

4. **Monitoring**: I log indicano chiaramente su quale canale avviene ogni connessione, facilitando il debugging.
